<script lang="ts">
    import { onMount } from 'svelte';
    import { apiFetch } from '$lib/api';
    import {
        Smartphone,
        PhoneCall,
        Database,
        TrendingUp,
        Clock,
        CheckCircle2,
        AlertCircle
    } from 'lucide-svelte';

    let stats = $state({
        registered_devices: 0,
        active_calls: 0,
        db_status: 'unknown'
    });
    let loading = $state(true);

    async function fetchStats() {
        try {
            stats = await apiFetch('/admin/stats');
        } catch (err) {
            console.error('Failed to fetch stats:', err);
        } finally {
            loading = false;
        }
    }

    $effect(() => {
        const interval = setInterval(fetchStats, 5000);
        fetchStats();
        return () => clearInterval(interval);
    });

    const cards = $derived([
        {
            label: 'Registered Devices',
            value: stats.registered_devices,
            icon: Smartphone,
            color: 'text-blue-500',
            bg: 'bg-blue-500/10',
            desc: 'Total active SIP registrations'
        },
        {
            label: 'Active Call Legs',
            value: stats.active_calls,
            icon: PhoneCall,
            color: 'text-emerald-500',
            bg: 'bg-emerald-500/10',
            desc: 'Calls currently being managed'
        },
        {
            label: 'Database Status',
            value: stats.db_status,
            icon: Database,
            color: stats.db_status === 'healthy' ? 'text-emerald-500' : 'text-red-500',
            bg: stats.db_status === 'healthy' ? 'bg-emerald-500/10' : 'bg-red-500/10',
            desc: 'Connectivity to PostgreSQL'
        }
    ]);
</script>

<div class="space-y-8">
    <div>
        <h1 class="text-3xl font-bold tracking-tight">System Overview</h1>
        <p class="text-zinc-400 mt-1">Real-time monitoring of your SIP infrastructure.</p>
    </div>

    <div class="grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
        {#each cards as card}
            <div class="rounded-xl border border-zinc-800 bg-zinc-900 p-6 shadow-sm">
                <div class="flex items-center justify-between">
                    <div class="rounded-lg {card.bg} p-2.5">
                        <card.icon class="h-6 w-6 {card.color}" />
                    </div>
                    {#if card.label === 'Active Call Legs' && card.value > 0}
                        <div class="flex items-center text-xs font-medium text-emerald-500 bg-emerald-500/10 px-2 py-1 rounded-full">
                            <TrendingUp class="h-3 w-3 mr-1" />
                            Live
                        </div>
                    {/if}
                </div>
                <div class="mt-4">
                    <h3 class="text-sm font-medium text-zinc-400 uppercase tracking-wider">{card.label}</h3>
                    <div class="mt-1 flex items-baseline">
                        <span class="text-3xl font-bold tracking-tight">
                            {#if card.label === 'Database Status'}
                                <span class="capitalize">{card.value}</span>
                            {:else}
                                {card.value.toLocaleString()}
                            {/if}
                        </span>
                    </div>
                    <p class="mt-2 text-xs text-zinc-500 flex items-center">
                        <Clock class="h-3 w-3 mr-1" />
                        Updated just now
                    </p>
                </div>
            </div>
        {/each}
    </div>

    <!-- Live Status Section -->
    <div class="rounded-xl border border-zinc-800 bg-zinc-900 overflow-hidden">
        <div class="border-b border-zinc-800 bg-zinc-900/50 px-6 py-4">
            <h3 class="font-semibold">System Health</h3>
        </div>
        <div class="p-6">
            <div class="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                <div class="flex items-center gap-4">
                    <div class="relative flex h-3 w-3">
                        <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                        <span class="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span>
                    </div>
                    <div>
                        <p class="text-sm font-medium">All systems operational</p>
                        <p class="text-xs text-zinc-500">Sentry v0.0.1 running at optimal performance</p>
                    </div>
                </div>
                <div class="flex items-center gap-2">
                    <div class="flex items-center gap-1.5 rounded-md bg-zinc-800 px-3 py-1.5 text-xs font-medium text-zinc-300 border border-zinc-700">
                        <CheckCircle2 class="h-3.5 w-3.5 text-emerald-500" />
                        API: Online
                    </div>
                    <div class="flex items-center gap-1.5 rounded-md bg-zinc-800 px-3 py-1.5 text-xs font-medium text-zinc-300 border border-zinc-700">
                        <CheckCircle2 class="h-3.5 w-3.5 text-emerald-500" />
                        SIP: Active
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
