<script lang="ts">
    import { auth } from '$lib/auth.svelte';
    import { goto } from '$app/navigation';

    $effect(() => {
        if (auth.initialized) {
            goto(auth.isAuthenticated() ? '/dashboard' : '/login');
        }
    });
</script>
