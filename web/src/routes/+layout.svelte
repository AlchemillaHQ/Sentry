<script lang="ts">
    import '../app.css';
    import { auth } from '$lib/auth.svelte';
    import { goto } from '$app/navigation';
    import { page } from '$app/state';

    let { children } = $props();

    $effect(() => {
        if (auth.initialized && !auth.isAuthenticated() && page.url.pathname !== '/login') {
            goto('/login');
        }
    });
</script>

<div class="min-h-screen bg-background font-sans antialiased">
    {#if auth.initialized}
        {@render children()}
    {:else}
        <div class="flex h-screen items-center justify-center">
            <div class="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
        </div>
    {/if}
</div>
