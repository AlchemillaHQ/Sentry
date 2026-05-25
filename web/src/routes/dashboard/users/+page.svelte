<script lang="ts">
    import { apiFetch } from '$lib/api';
    import {
        Users,
        UserPlus,
        Trash2,
        Shield,
        Eye,
        Search,
        X,
        Loader2
    } from 'lucide-svelte';

    let users = $state<any[]>([]);
    let loading = $state(true);
    let showCreateModal = $state(false);
    let searchQuery = $state('');

    // Form state
    let newUsername = $state('');
    let newPassword = $state('');
    let newRole = $state('admin');
    let creating = $state(false);
    let createError = $state<string | null>(null);

    async function fetchUsers() {
        try {
            users = await apiFetch('/admin/users');
        } catch (err) {
            console.error('Failed to fetch users:', err);
        } finally {
            loading = false;
        }
    }

    async function handleCreateUser(e: SubmitEvent) {
        e.preventDefault();
        creating = true;
        createError = null;

        try {
            await apiFetch('/admin/users', {
                method: 'POST',
                body: JSON.stringify({
                    username: newUsername,
                    password: newPassword,
                    role: newRole
                })
            });
            await fetchUsers();
            showCreateModal = false;
            newUsername = '';
            newPassword = '';
        } catch (err: any) {
            createError = err.message || 'Failed to create user';
        } finally {
            creating = false;
        }
    }

    async function handleDeleteUser(username: string) {
        if (!confirm(`Are you sure you want to delete user "${username}"?`)) return;

        try {
            await apiFetch(`/admin/users/${username}`, {
                method: 'DELETE'
            });
            await fetchUsers();
        } catch (err) {
            alert('Failed to delete user');
        }
    }

    const filteredUsers = $derived(
        users.filter(u => u.username.toLowerCase().includes(searchQuery.toLowerCase()))
    );

    $effect(() => {
        fetchUsers();
    });
</script>

<div class="space-y-8">
    <div class="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
            <h1 class="text-3xl font-bold tracking-tight text-white">User Management</h1>
            <p class="text-zinc-400 mt-1">Manage administrative access to the Sentry Dashboard.</p>
        </div>
        <button
            onclick={() => showCreateModal = true}
            class="inline-flex items-center justify-center rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 transition-colors shadow-sm shadow-emerald-900/20"
        >
            <UserPlus class="mr-2 h-4 w-4" />
            Add User
        </button>
    </div>

    <!-- Search and Filters -->
    <div class="flex items-center rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2">
        <Search class="h-5 w-5 text-zinc-500 mr-2" />
        <input
            type="text"
            bind:value={searchQuery}
            placeholder="Search users..."
            class="flex-1 bg-transparent border-none focus:outline-none text-sm text-white placeholder-zinc-500"
        />
    </div>

    <!-- Users Table -->
    <div class="rounded-xl border border-zinc-800 bg-zinc-900 overflow-hidden">
        <table class="w-full text-left border-collapse">
            <thead class="bg-zinc-900/50 border-b border-zinc-800">
                <tr>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider">User</th>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider">Role</th>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider">Created At</th>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider text-right">Actions</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-zinc-800">
                {#if loading}
                    <tr>
                        <td colspan="4" class="px-6 py-12 text-center text-zinc-500">
                            <Loader2 class="h-8 w-8 animate-spin mx-auto mb-2 text-emerald-500" />
                            Loading users...
                        </td>
                    </tr>
                {:else if filteredUsers.length === 0}
                    <tr>
                        <td colspan="4" class="px-6 py-12 text-center text-zinc-500 italic">
                            No users found.
                        </td>
                    </tr>
                {:else}
                    {#each filteredUsers as user}
                        <tr class="group hover:bg-zinc-800/30 transition-colors">
                            <td class="px-6 py-4">
                                <div class="flex items-center">
                                    <div class="h-9 w-9 rounded-full bg-zinc-800 flex items-center justify-center text-zinc-300 font-semibold border border-zinc-700">
                                        {user.username.charAt(0).toUpperCase()}
                                    </div>
                                    <div class="ml-4">
                                        <div class="text-sm font-medium text-white">{user.username}</div>
                                    </div>
                                </div>
                            </td>
                            <td class="px-6 py-4">
                                <div class="flex items-center">
                                    {#if user.role === 'admin'}
                                        <span class="inline-flex items-center rounded-full bg-emerald-900/20 px-2.5 py-0.5 text-xs font-medium text-emerald-500 border border-emerald-900/30">
                                            <Shield class="mr-1 h-3 w-3" />
                                            Admin
                                        </span>
                                    {:else}
                                        <span class="inline-flex items-center rounded-full bg-blue-900/20 px-2.5 py-0.5 text-xs font-medium text-blue-500 border border-blue-900/30">
                                            <Eye class="mr-1 h-3 w-3" />
                                            Viewer
                                        </span>
                                    {/if}
                                </div>
                            </td>
                            <td class="px-6 py-4 text-sm text-zinc-400">
                                {new Date(user.created_at).toLocaleDateString()}
                            </td>
                            <td class="px-6 py-4 text-right">
                                <button
                                    onclick={() => handleDeleteUser(user.username)}
                                    class="text-zinc-500 hover:text-red-400 p-2 rounded-md hover:bg-red-400/10 transition-all opacity-0 group-hover:opacity-100"
                                    title="Delete User"
                                >
                                    <Trash2 class="h-4 w-4" />
                                </button>
                            </td>
                        </tr>
                    {/each}
                {/if}
            </tbody>
        </table>
    </div>
</div>

<!-- Create User Modal -->
{#if showCreateModal}
    <div class="fixed inset-0 z-50 flex items-center justify-center px-4">
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div class="absolute inset-0 bg-black/80 backdrop-blur-sm" onclick={() => showCreateModal = false}></div>
        <div class="relative w-full max-w-md rounded-xl border border-zinc-800 bg-zinc-900 p-8 shadow-2xl">
            <div class="flex items-center justify-between mb-6">
                <h3 class="text-xl font-bold text-white">Add New User</h3>
                <button onclick={() => showCreateModal = false} class="text-zinc-500 hover:text-white">
                    <X class="h-5 w-5" />
                </button>
            </div>

            <form class="space-y-4" onsubmit={handleCreateUser}>
                <div>
                    <label for="new_username" class="block text-sm font-medium text-zinc-400 mb-1.5">Username</label>
                    <input
                        id="new_username"
                        type="text"
                        bind:value={newUsername}
                        required
                        class="w-full rounded-md border border-zinc-800 bg-zinc-950 px-3 py-2 text-white focus:border-emerald-500 focus:outline-none focus:ring-1 focus:ring-emerald-500 transition-all sm:text-sm"
                        placeholder="e.g. jdoe"
                    />
                </div>
                <div>
                    <label for="new_password" class="block text-sm font-medium text-zinc-400 mb-1.5">Password</label>
                    <input
                        id="new_password"
                        type="password"
                        bind:value={newPassword}
                        required
                        class="w-full rounded-md border border-zinc-800 bg-zinc-950 px-3 py-2 text-white focus:border-emerald-500 focus:outline-none focus:ring-1 focus:ring-emerald-500 transition-all sm:text-sm"
                        placeholder="••••••••"
                    />
                </div>
                <div>
                    <label for="new_role" class="block text-sm font-medium text-zinc-400 mb-1.5">Role</label>
                    <select
                        id="new_role"
                        bind:value={newRole}
                        class="w-full rounded-md border border-zinc-800 bg-zinc-950 px-3 py-2 text-white focus:border-emerald-500 focus:outline-none focus:ring-1 focus:ring-emerald-500 transition-all sm:text-sm"
                    >
                        <option value="admin">Administrator</option>
                        <option value="viewer">Viewer</option>
                    </select>
                </div>

                {#if createError}
                    <div class="rounded-md bg-red-900/20 p-3 border border-red-900/50">
                        <p class="text-sm text-red-500 text-center font-medium">{createError}</p>
                    </div>
                {/if}

                <div class="pt-4 flex gap-3">
                    <button
                        type="button"
                        onclick={() => showCreateModal = false}
                        class="flex-1 rounded-md border border-zinc-800 bg-zinc-900 px-4 py-2 text-sm font-medium text-zinc-300 hover:bg-zinc-800 transition-colors"
                    >
                        Cancel
                    </button>
                    <button
                        type="submit"
                        disabled={creating}
                        class="flex-1 inline-flex items-center justify-center rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 transition-colors shadow-sm disabled:opacity-50 shadow-emerald-900/20"
                    >
                        {#if creating}
                            <Loader2 class="mr-2 h-4 w-4 animate-spin" />
                        {/if}
                        Create User
                    </button>
                </div>
            </form>
        </div>
    </div>
{/if}
