<script lang="ts">
    import { auth } from '$lib/auth.svelte';
    import { apiFetch } from '$lib/api';
    import { goto } from '$app/navigation';
    import { ShieldCheck, Lock, User } from 'lucide-svelte';

    let username = $state('');
    let password = $state('');
    let loading = $state(false);
    let error = $state<string | null>(null);

    async function handleLogin(e: SubmitEvent) {
        e.preventDefault();
        loading = true;
        error = null;

        try {
            const data = await apiFetch('/auth/login', {
                method: 'POST',
                body: JSON.stringify({ username, password })
            });
            auth.login(data.token, data.user);
            goto('/dashboard');
        } catch (err: any) {
            error = err.message || 'Login failed';
        } finally {
            loading = false;
        }
    }
</script>

<div class="flex min-h-screen items-center justify-center bg-zinc-950 px-4 py-12 sm:px-6 lg:px-8">
    <div class="w-full max-w-md space-y-8">
        <div>
            <div class="mx-auto flex h-16 w-16 items-center justify-center rounded-full bg-zinc-900 border border-zinc-800">
                <ShieldCheck class="h-10 w-10 text-emerald-500" />
            </div>
            <h2 class="mt-6 text-center text-3xl font-extrabold tracking-tight text-white">
                Sentry Dashboard
            </h2>
            <p class="mt-2 text-center text-sm text-zinc-400">
                Sign in to manage your SIP infrastructure
            </p>
        </div>
        <form class="mt-8 space-y-6" onsubmit={handleLogin}>
            <div class="-space-y-px rounded-md shadow-sm">
                <div class="relative">
                    <User class="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-zinc-500" />
                    <input
                        id="username"
                        name="username"
                        type="text"
                        required
                        bind:value={username}
                        class="relative block w-full appearance-none rounded-none rounded-t-md border border-zinc-800 bg-zinc-900 px-10 py-3 text-white placeholder-zinc-500 focus:z-10 focus:border-emerald-500 focus:outline-none focus:ring-emerald-500 sm:text-sm"
                        placeholder="Username"
                    />
                </div>
                <div class="relative">
                    <Lock class="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-zinc-500" />
                    <input
                        id="password"
                        name="password"
                        type="password"
                        required
                        bind:value={password}
                        class="relative block w-full appearance-none rounded-none rounded-b-md border border-zinc-800 bg-zinc-900 px-10 py-3 text-white placeholder-zinc-500 focus:z-10 focus:border-emerald-500 focus:outline-none focus:ring-emerald-500 sm:text-sm"
                        placeholder="Password"
                    />
                </div>
            </div>

            {#if error}
                <div class="rounded-md bg-red-900/20 p-4 border border-red-900/50">
                    <p class="text-sm text-red-500 text-center font-medium">{error}</p>
                </div>
            {/if}

            <div>
                <button
                    type="submit"
                    disabled={loading}
                    class="group relative flex w-full justify-center rounded-md border border-transparent bg-emerald-600 px-4 py-3 text-sm font-medium text-white hover:bg-emerald-700 focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:ring-offset-2 focus:ring-offset-zinc-950 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
                >
                    {#if loading}
                        <div class="mr-2 h-5 w-5 animate-spin rounded-full border-2 border-white border-t-transparent"></div>
                    {/if}
                    Sign in
                </button>
            </div>
        </form>
    </div>
</div>
