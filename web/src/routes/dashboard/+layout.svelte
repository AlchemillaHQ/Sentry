<script lang="ts">
    import { auth } from '$lib/auth.svelte';
    import { page } from '$app/state';
    import {
        LayoutDashboard,
        Smartphone,
        Users,
        Settings,
        LogOut,
        ShieldCheck,
        Menu,
        X
    } from 'lucide-svelte';

    let { children } = $props();
    let sidebarOpen = $state(false);

    const menuItems = [
        { href: '/dashboard', label: 'Overview', icon: LayoutDashboard },
        { href: '/dashboard/devices', label: 'Devices', icon: Smartphone },
        { href: '/dashboard/users', label: 'User Management', icon: Users },
        { href: '/dashboard/settings', label: 'Settings', icon: Settings },
    ];

    function toggleSidebar() {
        sidebarOpen = !sidebarOpen;
    }
</script>

<div class="flex min-h-screen bg-zinc-950 text-white">
    <!-- Desktop Sidebar -->
    <aside class="hidden w-64 flex-col border-r border-zinc-800 bg-zinc-900 lg:flex">
        <div class="flex h-16 items-center px-6 border-b border-zinc-800">
            <ShieldCheck class="h-8 w-8 text-emerald-500 mr-2" />
            <span class="text-xl font-bold tracking-tight">SENTRY</span>
        </div>
        <nav class="flex-1 space-y-1 px-4 py-4">
            {#each menuItems as item}
                <a
                    href={item.href}
                    class="group flex items-center rounded-md px-3 py-2.5 text-sm font-medium transition-all duration-200 {page.url.pathname === item.href ? 'bg-zinc-800 text-emerald-400' : 'text-zinc-400 hover:bg-zinc-800 hover:text-white'}"
                >
                    <item.icon class="mr-3 h-5 w-5 shrink-0" />
                    {item.label}
                </a>
            {/each}
        </nav>
        <div class="border-t border-zinc-800 p-4">
            <div class="flex items-center px-3 py-2">
                <div class="h-8 w-8 rounded-full bg-zinc-800 flex items-center justify-center text-emerald-500 font-bold border border-zinc-700">
                    {auth.user?.username?.charAt(0).toUpperCase()}
                </div>
                <div class="ml-3 min-w-0 flex-1">
                    <p class="truncate text-sm font-medium">{auth.user?.username}</p>
                    <p class="truncate text-xs text-zinc-500 capitalize">{auth.user?.role}</p>
                </div>
                <button
                    onclick={() => auth.logout()}
                    class="ml-2 rounded-md p-1.5 text-zinc-500 hover:bg-zinc-800 hover:text-red-400 transition-colors"
                    title="Logout"
                >
                    <LogOut class="h-5 w-5" />
                </button>
            </div>
        </div>
    </aside>

    <!-- Mobile Header -->
    <div class="lg:hidden fixed top-0 left-0 right-0 z-20 bg-zinc-900 border-b border-zinc-800 px-4 h-16 flex items-center justify-between">
        <div class="flex items-center">
            <ShieldCheck class="h-7 w-7 text-emerald-500 mr-2" />
            <span class="text-lg font-bold tracking-tight">SENTRY</span>
        </div>
        <button onclick={toggleSidebar} class="p-2 text-zinc-400 hover:text-white">
            <Menu class="h-6 w-6" />
        </button>
    </div>

    <!-- Mobile Sidebar Overlay -->
    {#if sidebarOpen}
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
            class="fixed inset-0 z-30 bg-black/60 lg:hidden"
            onclick={toggleSidebar}
        ></div>
        <aside class="fixed inset-y-0 left-0 z-40 w-64 bg-zinc-900 shadow-xl lg:hidden transform transition-transform duration-300">
            <div class="flex h-16 items-center px-6 border-b border-zinc-800 justify-between">
                <div class="flex items-center">
                    <ShieldCheck class="h-8 w-8 text-emerald-500 mr-2" />
                    <span class="text-xl font-bold tracking-tight">SENTRY</span>
                </div>
                <button onclick={toggleSidebar} class="p-1 text-zinc-500 hover:text-white">
                    <X class="h-6 w-6" />
                </button>
            </div>
            <nav class="space-y-1 px-4 py-4">
                {#each menuItems as item}
                    <a
                        href={item.href}
                        onclick={toggleSidebar}
                        class="group flex items-center rounded-md px-3 py-3 text-base font-medium transition-all {page.url.pathname === item.href ? 'bg-zinc-800 text-emerald-400' : 'text-zinc-400 hover:bg-zinc-800 hover:text-white'}"
                    >
                        <item.icon class="mr-4 h-6 w-6 shrink-0" />
                        {item.label}
                    </a>
                {/each}
            </nav>
            <div class="absolute bottom-0 w-full border-t border-zinc-800 p-4">
                <button
                    onclick={() => auth.logout()}
                    class="flex w-full items-center px-3 py-3 text-base font-medium text-zinc-400 hover:bg-zinc-800 hover:text-red-400 rounded-md transition-colors"
                >
                    <LogOut class="mr-4 h-6 w-6" />
                    Logout
                </button>
            </div>
        </aside>
    {/if}

    <!-- Main Content -->
    <main class="flex-1 lg:pl-0 pt-16 lg:pt-0 overflow-y-auto">
        <div class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
            {@render children()}
        </div>
    </main>
</div>
