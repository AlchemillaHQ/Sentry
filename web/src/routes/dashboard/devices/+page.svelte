<script lang="ts">
	import { apiFetch } from '$lib/api';
	import {
		Activity,
		Bell,
		CalendarDays,
		Check,
		CheckCircle2,
		ChevronDown,
		CircleAlert,
		Clock,
		Copy,
		Globe,
		Loader2,
		RefreshCcw,
		Search,
		Server,
		Smartphone,
		Sparkles,
		Trash2,
		UserRound,
		WifiOff
	} from 'lucide-svelte';

	type RegistrationHealth = {
		managed: boolean;
		registered: boolean;
		pending: boolean;
		state: string;
		gateway_state: string;
		probe_mode: string;
		retry_attempts: number;
		last_error?: string;
		last_success?: string;
		sip_expires_at?: string;
		gateway_last_probe_at?: string;
		gateway_last_probe_rtt_ms?: number;
		gateway_last_sip_at?: string;
	};

	type Device = {
		device_id: string;
		platform: string;
		upstream_host: string;
		upstream_port: number;
		upstream_transport: string;
		upstream_user: string;
		upstream_realm?: string;
		display_name?: string;
		b2bua_sip_user: string;
		device_contact?: string;
		user_agent?: string;
		push_provider?: string;
		push_configured: boolean;
		registered_at: string;
		expires_at: string;
		last_seen: string;
		disabled: boolean;
		registration: RegistrationHealth;
	};

	type DeviceFilter = 'all' | 'healthy' | 'attention' | 'disabled';

	let devices = $state<Device[]>([]);
	let loading = $state(true);
	let refreshing = $state(false);
	let searchQuery = $state('');
	let statusFilter = $state<DeviceFilter>('all');
	let purging = $state(false);
	let actionDevice = $state<string | null>(null);
	let expandedDevice = $state<string | null>(null);
	let copiedDevice = $state<string | null>(null);
	let loadError = $state<string | null>(null);

	function registrationState(device: Device) {
		if (device.disabled) return 'disabled';
		return device.registration?.state || 'unknown';
	}

	function isHealthy(device: Device) {
		const state = registrationState(device);
		return state === 'registered' || state === 'refreshing';
	}

	function needsAttention(device: Device) {
		return !device.disabled && !isHealthy(device);
	}

	function matchesFilter(device: Device) {
		if (statusFilter === 'healthy') return isHealthy(device);
		if (statusFilter === 'attention') return needsAttention(device);
		if (statusFilter === 'disabled') return device.disabled;
		return true;
	}

	const filteredDevices = $derived(
		devices.filter((device) => {
			const query = searchQuery.trim().toLowerCase();
			const matchesSearch =
				query.length === 0 ||
				[
					device.display_name,
					device.device_id,
					device.upstream_user,
					device.upstream_host,
					device.upstream_port?.toString(),
					device.upstream_transport,
					device.platform,
					device.user_agent,
					device.b2bua_sip_user
				]
					.filter(Boolean)
					.join(' ')
					.toLowerCase()
					.includes(query);
			return matchesSearch && matchesFilter(device);
		})
	);

	const healthyCount = $derived(devices.filter(isHealthy).length);
	const attentionCount = $derived(devices.filter(needsAttention).length);
	const disabledCount = $derived(devices.filter((device) => device.disabled).length);

	async function fetchDevices(showRefresh = false) {
		if (showRefresh) refreshing = true;
		try {
			devices = await apiFetch('/admin/devices');
			loadError = null;
			if (expandedDevice && !devices.some((device) => device.device_id === expandedDevice)) {
				expandedDevice = null;
			}
		} catch (err) {
			loadError = err instanceof Error ? err.message : 'Failed to load devices';
			console.error('Failed to fetch devices:', err);
		} finally {
			loading = false;
			refreshing = false;
		}
	}

	async function handleReregister(deviceId: string) {
		actionDevice = deviceId;
		try {
			await apiFetch(`/devices/${deviceId}/reregister`, { method: 'POST' });
			await fetchDevices();
		} catch (err) {
			alert(err instanceof Error ? err.message : 'Failed to trigger re-registration');
		} finally {
			actionDevice = null;
		}
	}

	async function handleDeleteDevice(deviceId: string) {
		if (!confirm('Are you sure you want to remove this device registration?')) return;
		actionDevice = deviceId;
		try {
			await apiFetch(`/devices/${deviceId}`, { method: 'DELETE' });
			await fetchDevices();
		} catch (err) {
			alert(err instanceof Error ? err.message : 'Failed to remove device');
		} finally {
			actionDevice = null;
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
			alert(err instanceof Error ? err.message : 'Failed to purge junk devices');
		} finally {
			purging = false;
		}
	}

	async function copyDeviceId(deviceId: string) {
		try {
			await navigator.clipboard.writeText(deviceId);
			copiedDevice = deviceId;
			setTimeout(() => {
				if (copiedDevice === deviceId) copiedDevice = null;
			}, 1500);
		} catch {
			alert('Unable to copy the device ID');
		}
	}

	function toggleDetails(deviceId: string) {
		expandedDevice = expandedDevice === deviceId ? null : deviceId;
	}

	function displayName(device: Device) {
		return device.display_name?.trim() || `Extension ${device.upstream_user}`;
	}

	function initials(device: Device) {
		const value = device.display_name?.trim() || device.upstream_user;
		return value
			.split(/\s+/)
			.slice(0, 2)
			.map((part) => part.charAt(0).toUpperCase())
			.join('');
	}

	function compactId(value: string) {
		if (value.length <= 16) return value;
		return `${value.slice(0, 8)}…${value.slice(-4)}`;
	}

	function statusLabel(device: Device) {
		const labels: Record<string, string> = {
			disabled: 'Disabled',
			registered: 'Registered',
			refreshing: 'Refreshing',
			registering: 'Registering',
			queued: 'Queued',
			retrying: 'Retrying',
			pending: 'Pending',
			gateway_suspect: 'Gateway suspect',
			gateway_unavailable: 'Gateway unavailable',
			unmanaged: 'Unmanaged',
			unknown: 'Unknown'
		};
		return labels[registrationState(device)] || registrationState(device).replaceAll('_', ' ');
	}

	function statusClasses(device: Device) {
		const state = registrationState(device);
		if (state === 'registered') {
			return 'border-emerald-800/60 bg-emerald-950/60 text-emerald-300';
		}
		if (state === 'refreshing') {
			return 'border-cyan-800/60 bg-cyan-950/60 text-cyan-300';
		}
		if (state === 'gateway_unavailable') {
			return 'border-red-800/60 bg-red-950/60 text-red-300';
		}
		if (state === 'disabled' || state === 'unmanaged' || state === 'unknown') {
			return 'border-zinc-700 bg-zinc-800/70 text-zinc-300';
		}
		return 'border-amber-800/60 bg-amber-950/60 text-amber-300';
	}

	function gatewayLabel(state?: string) {
		if (state === 'available') return 'Gateway available';
		if (state === 'suspect') return 'Gateway suspect';
		if (state === 'unavailable') return 'Gateway unavailable';
		return 'Gateway unknown';
	}

	function probeModeLabel(mode?: string) {
		if (mode === 'options') return 'SIP OPTIONS';
		if (mode === 'register_canary') return 'Shared REGISTER canary';
		if (mode === 'disabled') return 'Active probes disabled';
		return 'Unknown';
	}

	function formatTimestamp(value?: string) {
		if (!value) return 'Not available';
		const date = new Date(value);
		if (Number.isNaN(date.getTime())) return 'Not available';
		return date.toLocaleString([], {
			year: 'numeric',
			month: 'short',
			day: 'numeric',
			hour: '2-digit',
			minute: '2-digit',
			second: '2-digit'
		});
	}

	function relativeTime(value?: string) {
		if (!value) return 'Never';
		const timestamp = new Date(value).getTime();
		if (Number.isNaN(timestamp)) return 'Never';
		const seconds = Math.round((Date.now() - timestamp) / 1000);
		const future = seconds < 0;
		const absolute = Math.abs(seconds);
		let amount: number;
		let unit: string;
		if (absolute < 60) {
			amount = absolute;
			unit = 'sec';
		} else if (absolute < 3600) {
			amount = Math.round(absolute / 60);
			unit = 'min';
		} else if (absolute < 86400) {
			amount = Math.round(absolute / 3600);
			unit = 'hr';
		} else {
			amount = Math.round(absolute / 86400);
			unit = 'day';
		}
		return future ? `in ${amount} ${unit}` : `${amount} ${unit} ago`;
	}

	$effect(() => {
		const interval = setInterval(() => fetchDevices(), 30000);
		fetchDevices();
		return () => clearInterval(interval);
	});
</script>

<div class="space-y-6">
	<div class="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
		<div>
			<h1 class="text-3xl font-bold tracking-tight text-white">Devices</h1>
			<p class="mt-1 text-zinc-400">
				Account identity, upstream registration health, and mobile delivery details.
			</p>
		</div>
		<div class="flex flex-wrap gap-2">
			<button
				onclick={handlePurgeJunk}
				disabled={purging}
				class="inline-flex items-center justify-center rounded-md border border-amber-800 bg-amber-900/30 px-3 py-2 text-sm font-medium text-amber-300 transition-colors hover:bg-amber-900/50 disabled:opacity-50"
			>
				<Sparkles class="mr-2 h-4 w-4" />
				{purging ? 'Purging…' : 'Purge junk'}
			</button>
			<button
				onclick={() => fetchDevices(true)}
				disabled={refreshing}
				class="inline-flex items-center justify-center rounded-md border border-zinc-800 bg-zinc-900 px-3 py-2 text-sm font-medium text-zinc-300 transition-colors hover:bg-zinc-800 disabled:opacity-50"
			>
				<RefreshCcw class={`mr-2 h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
				Refresh
			</button>
		</div>
	</div>

	<div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
		<div
			class="flex min-w-0 flex-1 items-center rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2 lg:max-w-xl"
		>
			<Search class="mr-2 h-5 w-5 shrink-0 text-zinc-500" />
			<input
				type="text"
				bind:value={searchQuery}
				placeholder="Search name, extension, host, device, or user agent…"
				class="min-w-0 flex-1 border-none bg-transparent text-sm text-white placeholder-zinc-500 focus:outline-none"
			/>
		</div>
		<div class="flex flex-wrap gap-2 text-xs">
			<button
				onclick={() => (statusFilter = 'all')}
				class={`rounded-full border px-3 py-1.5 transition-colors ${statusFilter === 'all' ? 'border-zinc-600 bg-zinc-700 text-white' : 'border-zinc-800 bg-zinc-900 text-zinc-400 hover:text-white'}`}
				>All <span class="ml-1 text-zinc-500">{devices.length}</span></button
			>
			<button
				onclick={() => (statusFilter = 'healthy')}
				class={`rounded-full border px-3 py-1.5 transition-colors ${statusFilter === 'healthy' ? 'border-emerald-700 bg-emerald-950 text-emerald-300' : 'border-zinc-800 bg-zinc-900 text-zinc-400 hover:text-white'}`}
				>Healthy <span class="ml-1 text-emerald-500/70">{healthyCount}</span></button
			>
			<button
				onclick={() => (statusFilter = 'attention')}
				class={`rounded-full border px-3 py-1.5 transition-colors ${statusFilter === 'attention' ? 'border-amber-700 bg-amber-950 text-amber-300' : 'border-zinc-800 bg-zinc-900 text-zinc-400 hover:text-white'}`}
				>Attention <span class="ml-1 text-amber-500/70">{attentionCount}</span></button
			>
			<button
				onclick={() => (statusFilter = 'disabled')}
				class={`rounded-full border px-3 py-1.5 transition-colors ${statusFilter === 'disabled' ? 'border-zinc-600 bg-zinc-800 text-zinc-200' : 'border-zinc-800 bg-zinc-900 text-zinc-400 hover:text-white'}`}
				>Disabled <span class="ml-1 text-zinc-500">{disabledCount}</span></button
			>
		</div>
	</div>

	{#if loadError}
		<div
			class="flex items-center rounded-lg border border-red-900/70 bg-red-950/40 px-4 py-3 text-sm text-red-300"
		>
			<CircleAlert class="mr-2 h-4 w-4 shrink-0" />
			{loadError}
		</div>
	{/if}

	<div class="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900">
		<div class="overflow-x-auto">
			<table class="w-full min-w-[980px] border-collapse text-left text-sm">
				<thead class="border-b border-zinc-800 bg-zinc-950/40">
					<tr>
						<th class="px-5 py-3.5 text-xs font-semibold tracking-wider text-zinc-400 uppercase"
							>Account</th
						>
						<th class="px-5 py-3.5 text-xs font-semibold tracking-wider text-zinc-400 uppercase"
							>Registration</th
						>
						<th class="px-5 py-3.5 text-xs font-semibold tracking-wider text-zinc-400 uppercase"
							>Gateway</th
						>
						<th class="px-5 py-3.5 text-xs font-semibold tracking-wider text-zinc-400 uppercase"
							>Activity</th
						>
						<th
							class="px-5 py-3.5 text-right text-xs font-semibold tracking-wider text-zinc-400 uppercase"
							>Actions</th
						>
					</tr>
				</thead>
				<tbody class="divide-y divide-zinc-800">
					{#if loading && devices.length === 0}
						<tr>
							<td colspan="5" class="px-6 py-14 text-center text-zinc-500">
								<Loader2 class="mx-auto mb-2 h-8 w-8 animate-spin text-emerald-500" />
								Loading device health…
							</td>
						</tr>
					{:else if filteredDevices.length === 0}
						<tr>
							<td colspan="5" class="px-6 py-14 text-center text-zinc-500">
								No devices match this view.
							</td>
						</tr>
					{:else}
						{#each filteredDevices as device (device.device_id)}
							<tr class="group transition-colors hover:bg-zinc-800/30">
								<td class="px-5 py-4">
									<div class="flex items-center gap-3">
										<div
											class="flex h-10 w-10 shrink-0 items-center justify-center rounded-full border border-zinc-700 bg-zinc-800 text-xs font-semibold text-zinc-200"
										>
											{initials(device)}
										</div>
										<div class="min-w-0">
											<div
												class="max-w-[240px] truncate font-medium text-white"
												title={displayName(device)}
											>
												{displayName(device)}
											</div>
											<div class="mt-0.5 flex items-center gap-1.5 text-xs text-zinc-400">
												<span class="font-mono text-zinc-300">{device.upstream_user}</span>
												<span class="text-zinc-700">•</span>
												<span class="capitalize">{device.platform}</span>
											</div>
											<div
												class="mt-1 font-mono text-[10px] text-zinc-600"
												title={device.device_id}
											>
												{compactId(device.device_id)}
											</div>
										</div>
									</div>
								</td>
								<td class="px-5 py-4">
									<div class="flex flex-col items-start gap-1.5">
										<span
											class={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium ${statusClasses(device)}`}
										>
											{#if isHealthy(device)}
												<CheckCircle2 class="mr-1.5 h-3 w-3" />
											{:else if registrationState(device) === 'gateway_unavailable'}
												<WifiOff class="mr-1.5 h-3 w-3" />
											{:else}
												<Activity class="mr-1.5 h-3 w-3" />
											{/if}
											{statusLabel(device)}
										</span>
										{#if device.registration?.last_error}
											<span
												class="max-w-[220px] truncate text-[11px] text-red-400/80"
												title={device.registration.last_error}
												>{device.registration.last_error}</span
											>
										{:else if device.registration?.managed}
											<span class="text-[11px] text-zinc-500">Managed by Sentry</span>
										{:else}
											<span class="text-[11px] text-zinc-500">No runtime registration</span>
										{/if}
									</div>
								</td>
								<td class="px-5 py-4">
									<div class="max-w-[260px]">
										<div
											class="flex items-center truncate text-zinc-200"
											title={device.upstream_host}
										>
											<Globe class="mr-1.5 h-3.5 w-3.5 shrink-0 text-zinc-500" />
											<span class="truncate">{device.upstream_host}</span>
										</div>
										<div class="mt-1.5 flex items-center gap-2 text-[11px] text-zinc-500">
											<span
												class="rounded border border-zinc-800 bg-zinc-950 px-1.5 py-0.5 uppercase"
												>{device.upstream_transport || 'unknown'}</span
											>
											<span>Port {device.upstream_port || '—'}</span>
											<span
												class={device.registration?.gateway_state === 'available'
													? 'text-emerald-500'
													: device.registration?.gateway_state === 'unavailable'
														? 'text-red-400'
														: 'text-amber-400'}
												title={gatewayLabel(device.registration?.gateway_state)}>●</span
											>
										</div>
									</div>
								</td>
								<td class="px-5 py-4">
									<div
										class="flex items-center text-zinc-300"
										title={formatTimestamp(device.last_seen)}
									>
										<Clock class="mr-1.5 h-3.5 w-3.5 text-zinc-500" />
										{relativeTime(device.last_seen)}
									</div>
									<div class="mt-1.5 text-[11px] text-zinc-600">
										{formatTimestamp(device.last_seen)}
									</div>
								</td>
								<td class="px-5 py-4 text-right">
									<div class="flex justify-end gap-1">
										<button
											onclick={() => toggleDetails(device.device_id)}
											class="rounded-md p-2 text-zinc-500 transition-all hover:bg-zinc-700/60 hover:text-white"
											title="Show device details"
											aria-label="Show device details"
											aria-expanded={expandedDevice === device.device_id}
										>
											<ChevronDown
												class={`h-4 w-4 transition-transform ${expandedDevice === device.device_id ? 'rotate-180' : ''}`}
											/>
										</button>
										<button
											onclick={() => handleReregister(device.device_id)}
											disabled={device.disabled || actionDevice === device.device_id}
											class="rounded-md p-2 text-zinc-500 opacity-60 transition-all group-hover:opacity-100 hover:bg-emerald-400/10 hover:text-emerald-400 disabled:cursor-not-allowed disabled:opacity-25"
											title={device.disabled
												? 'Enable the device before re-registering'
												: 'Force upstream re-registration'}
											aria-label="Force upstream re-registration"
										>
											<RefreshCcw
												class={`h-4 w-4 ${actionDevice === device.device_id ? 'animate-spin' : ''}`}
											/>
										</button>
										<button
											onclick={() => handleDeleteDevice(device.device_id)}
											disabled={actionDevice === device.device_id}
											class="rounded-md p-2 text-zinc-500 opacity-60 transition-all group-hover:opacity-100 hover:bg-red-400/10 hover:text-red-400 disabled:opacity-25"
											title="Remove device"
											aria-label="Remove device"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</div>
								</td>
							</tr>
							{#if expandedDevice === device.device_id}
								<tr class="bg-zinc-950/55">
									<td colspan="5" class="px-5 py-5">
										<div class="grid gap-4 xl:grid-cols-3">
											<section class="rounded-lg border border-zinc-800 bg-zinc-900/70 p-4">
												<h3
													class="mb-3 flex items-center text-xs font-semibold tracking-wider text-zinc-400 uppercase"
												>
													<UserRound class="mr-2 h-4 w-4 text-emerald-500" />Account and routing
												</h3>
												<dl class="space-y-3 text-sm">
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															Display name
														</dt>
														<dd class="mt-0.5 text-zinc-200">{displayName(device)}</dd>
													</div>
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															Upstream identity
														</dt>
														<dd class="mt-0.5 font-mono text-xs break-all text-zinc-300">
															sip:{device.upstream_user}@{device.upstream_host}:{device.upstream_port};transport={device.upstream_transport}
														</dd>
													</div>
													<div class="grid grid-cols-2 gap-3">
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Realm
															</dt>
															<dd
																class="mt-0.5 truncate text-zinc-300"
																title={device.upstream_realm || 'Default'}
															>
																{device.upstream_realm || 'Default'}
															</dd>
														</div>
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																B2BUA user
															</dt>
															<dd
																class="mt-0.5 truncate font-mono text-xs text-zinc-300"
																title={device.b2bua_sip_user}
															>
																{device.b2bua_sip_user || '—'}
															</dd>
														</div>
													</div>
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															Learned contact
														</dt>
														<dd class="mt-0.5 font-mono text-xs break-all text-zinc-400">
															{device.device_contact || 'Not learned yet'}
														</dd>
													</div>
												</dl>
											</section>

											<section class="rounded-lg border border-zinc-800 bg-zinc-900/70 p-4">
												<h3
													class="mb-3 flex items-center text-xs font-semibold tracking-wider text-zinc-400 uppercase"
												>
													<Server class="mr-2 h-4 w-4 text-cyan-500" />Live registration
												</h3>
												<dl class="space-y-3 text-sm">
													<div class="grid grid-cols-2 gap-3">
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Account state
															</dt>
															<dd class="mt-0.5 text-zinc-200">{statusLabel(device)}</dd>
														</div>
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Gateway
															</dt>
															<dd class="mt-0.5 text-zinc-200">
																{gatewayLabel(device.registration?.gateway_state)}
															</dd>
														</div>
													</div>
													<div class="grid grid-cols-2 gap-3">
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Health mode
															</dt>
															<dd class="mt-0.5 text-zinc-300">
																{probeModeLabel(device.registration?.probe_mode)}
															</dd>
														</div>
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Retries
															</dt>
															<dd class="mt-0.5 text-zinc-300">
																{device.registration?.retry_attempts || 0}
															</dd>
														</div>
													</div>
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															Last registration success
														</dt>
														<dd class="mt-0.5 text-zinc-300">
															{formatTimestamp(device.registration?.last_success)}
														</dd>
													</div>
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															SIP registration expires
														</dt>
														<dd class="mt-0.5 text-zinc-300">
															{formatTimestamp(device.registration?.sip_expires_at)}
														</dd>
													</div>
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															Last gateway probe
														</dt>
														<dd class="mt-0.5 text-zinc-300">
															{formatTimestamp(device.registration?.gateway_last_probe_at)}
															{#if device.registration?.gateway_last_probe_rtt_ms}
																<span class="text-zinc-600">
																	· {device.registration.gateway_last_probe_rtt_ms} ms</span
																>
															{/if}
														</dd>
													</div>
												</dl>
											</section>

											<section class="rounded-lg border border-zinc-800 bg-zinc-900/70 p-4">
												<h3
													class="mb-3 flex items-center text-xs font-semibold tracking-wider text-zinc-400 uppercase"
												>
													<Smartphone class="mr-2 h-4 w-4 text-violet-500" />Device and delivery
												</h3>
												<dl class="space-y-3 text-sm">
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															Device ID
														</dt>
														<dd class="mt-1 flex items-center gap-2">
															<span
																class="min-w-0 truncate font-mono text-xs text-zinc-300"
																title={device.device_id}>{device.device_id}</span
															>
															<button
																onclick={() => copyDeviceId(device.device_id)}
																class="shrink-0 rounded p-1 text-zinc-500 hover:bg-zinc-800 hover:text-white"
																title="Copy device ID"
															>
																{#if copiedDevice === device.device_id}<Check
																		class="h-3.5 w-3.5 text-emerald-400"
																	/>{:else}<Copy class="h-3.5 w-3.5" />{/if}
															</button>
														</dd>
													</div>
													<div class="grid grid-cols-2 gap-3">
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Platform
															</dt>
															<dd class="mt-0.5 text-zinc-300 capitalize">{device.platform}</dd>
														</div>
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Push delivery
															</dt>
															<dd class="mt-0.5 flex items-center text-zinc-300">
																<Bell class="mr-1.5 h-3 w-3 text-zinc-500" />
																{device.push_configured
																	? device.push_provider || 'Configured'
																	: 'Not configured'}
															</dd>
														</div>
													</div>
													<div>
														<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
															User agent
														</dt>
														<dd class="mt-0.5 text-xs break-all text-zinc-400">
															{device.user_agent || 'Not reported'}
														</dd>
													</div>
													<div class="grid grid-cols-2 gap-3">
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																First registered
															</dt>
															<dd class="mt-0.5 text-xs text-zinc-400">
																{formatTimestamp(device.registered_at)}
															</dd>
														</div>
														<div>
															<dt class="text-[11px] tracking-wide text-zinc-600 uppercase">
																Record expires
															</dt>
															<dd class="mt-0.5 text-xs text-zinc-400">
																{formatTimestamp(device.expires_at)}
															</dd>
														</div>
													</div>
												</dl>
											</section>
										</div>

										{#if device.registration?.last_error}
											<div class="mt-4 rounded-lg border border-red-900/60 bg-red-950/30 px-4 py-3">
												<div class="flex items-start gap-2">
													<CircleAlert class="mt-0.5 h-4 w-4 shrink-0 text-red-400" />
													<div class="min-w-0">
														<p class="text-xs font-semibold tracking-wide text-red-400 uppercase">
															Last registration error
														</p>
														<p class="mt-1 font-mono text-xs leading-5 break-all text-red-300/80">
															{device.registration.last_error}
														</p>
													</div>
												</div>
											</div>
										{/if}

										<div
											class="mt-4 flex flex-wrap items-center gap-x-5 gap-y-2 text-[11px] text-zinc-600"
										>
											<span class="flex items-center"
												><CalendarDays class="mr-1.5 h-3.5 w-3.5" />Last seen {formatTimestamp(
													device.last_seen
												)}</span
											>
											<span class="flex items-center"
												><Activity class="mr-1.5 h-3.5 w-3.5" />Last gateway SIP {formatTimestamp(
													device.registration?.gateway_last_sip_at
												)}</span
											>
											<span class="flex items-center"
												><Globe
													class="mr-1.5 h-3.5 w-3.5"
												/>{device.upstream_transport?.toUpperCase()}
												{device.upstream_host}:{device.upstream_port}</span
											>
										</div>
									</td>
								</tr>
							{/if}
						{/each}
					{/if}
				</tbody>
			</table>
		</div>
	</div>
</div>
