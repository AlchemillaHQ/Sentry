<script lang="ts">
    import { apiFetch } from '$lib/api';
    import {
        Smartphone,
        Search,
        RefreshCcw,
        Trash2,
        Loader2,
        CheckCircle2,
        Clock,
        Globe,
        Sparkles
    } from 'lucide-svelte';

    let devices = $state<any[]>([]);
    let loading = $state(true);
    let searchQuery = $state('');
    let purging = $state(false);

    async function fetchDevices() {
        try {
            // For now, let's assume we use the mobile status endpoint or add a ListDevices one.
            // I'll add a ListDevices endpoint to the backend.
            devices = await apiFetch('/admin/devices');
        } catch (err) {
            console.error('Failed to fetch devices:', err);
        } finally {
            loading = false;
        }
    }

    async function handleReregister(deviceId: string) {
        try {
            await apiFetch(`/devices/${deviceId}/reregister`, { method: 'POST' });
            await fetchDevices();
        } catch (err) {
            alert('Failed to trigger re-registration');
        }
    }

    async function handleDeleteDevice(deviceId: string) {
        if (!confirm('Are you sure you want to remove this device registration?')) return;
        try {
            await apiFetch(`/devices/${deviceId}`, { method: 'DELETE' });
            await fetchDevices();
        } catch (err) {
            alert('Failed to remove device');
        }
    }

    async function handlePurgeJunk() {
        if (!confirm('Remove all devices with invalid hostnames (non-FQDN, non-IP)?')) return;
        purging = true;
        try {
            const result = await apiFetch('/admin/devices/cleanup-junk', { method: 'POST' });
            alert(`Purged ${result.deleted} junk device(s)`);
            await fetchDevices();
        } catch (err) {
            alert('Failed to purge junk devices');
        } finally {
            purging = false;
        }
    }

    const filteredDevices = $derived(
        devices.filter(d =>
            d.device_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
            d.upstream_user.toLowerCase().includes(searchQuery.toLowerCase())
        )
    );

    $effect(() => {
        const interval = setInterval(fetchDevices, 30000);
        fetchDevices();
        return () => clearInterval(interval);
    });
</script>

<div class="space-y-8">
    <div class="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
            <h1 class="text-3xl font-bold tracking-tight text-white">Registered Devices</h1>
            <p class="text-zinc-400 mt-1">Monitor and manage active mobile SIP registrations.</p>
        </div>
        <div class="flex gap-2">
            <button
                onclick={handlePurgeJunk}
                disabled={purging}
                class="inline-flex items-center justify-center rounded-md border border-amber-800 bg-amber-900/30 px-4 py-2 text-sm font-medium text-amber-300 hover:bg-amber-900/50 transition-colors disabled:opacity-50"
            >
                <Sparkles class="mr-2 h-4 w-4" />
                {purging ? 'Purging...' : 'Purge Junk'}
            </button>
            <button
                onclick={fetchDevices}
                class="inline-flex items-center justify-center rounded-md border border-zinc-800 bg-zinc-900 px-4 py-2 text-sm font-medium text-zinc-300 hover:bg-zinc-800 transition-colors"
            >
                <RefreshCcw class="mr-2 h-4 w-4" />
                Refresh
            </button>
        </div>
    </div>

    <div class="flex items-center rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2">
        <Search class="h-5 w-5 text-zinc-500 mr-2" />
        <input
            type="text"
            bind:value={searchQuery}
            placeholder="Search by User or Device ID..."
            class="flex-1 bg-transparent border-none focus:outline-none text-sm text-white placeholder-zinc-500"
        />
    </div>

    <div class="rounded-xl border border-zinc-800 bg-zinc-900 overflow-hidden">
        <table class="w-full text-left border-collapse text-sm">
            <thead class="bg-zinc-900/50 border-b border-zinc-800">
                <tr>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider">Identity</th>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider">Platform</th>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider">Upstream PBX</th>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider">Last Seen</th>
                    <th class="px-6 py-4 text-xs font-semibold text-zinc-400 uppercase tracking-wider text-right">Actions</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-zinc-800">
                {#if loading && devices.length === 0}
                    <tr>
                        <td colspan="5" class="px-6 py-12 text-center text-zinc-500">
                            <Loader2 class="h-8 w-8 animate-spin mx-auto mb-2 text-emerald-500" />
                            Loading devices...
                        </td>
                    </tr>
                {:else if filteredDevices.length === 0}
                    <tr>
                        <td colspan="5" class="px-6 py-12 text-center text-zinc-500 italic">
                            No active registrations found.
                        </td>
                    </tr>
                {:else}
                    {#each filteredDevices as device}
                        <tr class="group hover:bg-zinc-800/30 transition-colors">
                            <td class="px-6 py-4">
                                <div class="flex flex-col">
                                    <span class="font-medium text-white">{device.upstream_user}</span>
                                    <span class="text-xs text-zinc-500 font-mono mt-0.5 truncate max-w-[150px]">{device.device_id}</span>
                                </div>
                            </td>
                            <td class="px-6 py-4">
                                <span class="inline-flex items-center rounded-md bg-zinc-800 px-2 py-1 text-xs font-medium text-zinc-300 border border-zinc-700 capitalize">
                                    <Smartphone class="mr-1.5 h-3 w-3" />
                                    {device.platform}
                                </span>
                            </td>
                            <td class="px-6 py-4">
                                <div class="flex flex-col">
                                    <span class="text-zinc-300 flex items-center">
                                        <Globe class="mr-1.5 h-3 w-3 text-zinc-500" />
                                        {device.upstream_host}
                                    </span>
                                    <span class="text-[10px] text-emerald-500 font-medium mt-1 uppercase tracking-tight flex items-center">
                                        <CheckCircle2 class="mr-1 h-2.5 w-2.5" />
                                        Registered
                                    </span>
                                </div>
                            </td>
                            <td class="px-6 py-4 text-zinc-400">
                                <div class="flex items-center">
                                    <Clock class="mr-1.5 h-3 w-3" />
                                    {new Date(device.last_seen).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                </div>
                            </td>
                            <td class="px-6 py-4 text-right">
                                <div class="flex justify-end gap-2">
                                    <button
                                        onclick={() => handleReregister(device.device_id)}
                                        class="text-zinc-500 hover:text-emerald-400 p-2 rounded-md hover:bg-emerald-400/10 transition-all opacity-0 group-hover:opacity-100"
                                        title="Force Upstream Re-registration"
                                    >
                                        <RefreshCcw class="h-4 w-4" />
                                    </button>
                                    <button
                                        onclick={() => handleDeleteDevice(device.device_id)}
                                        class="text-zinc-500 hover:text-red-400 p-2 rounded-md hover:bg-red-400/10 transition-all opacity-0 group-hover:opacity-100"
                                        title="Remove Device"
                                    >
                                        <Trash2 class="h-4 w-4" />
                                    </button>
                                </div>
                            </td>
                        </tr>
                    {/each}
                {/if}
            </tbody>
        </table>
    </div>
</div>
