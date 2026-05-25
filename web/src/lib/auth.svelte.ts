import { browser } from '$app/environment';
import { goto } from '$app/navigation';

class AuthState {
    user = $state<{ username: string; role: string } | null>(null);
    token = $state<string | null>(browser ? localStorage.getItem('sentry_token') : null);
    initialized = $state(false);

    constructor() {
        if (browser && this.token) {
            this.init();
        } else {
            this.initialized = true;
        }
    }

    async init() {
        if (!this.token) return;

        try {
            // We'll add an endpoint to verify token / get user info
            // For now, let's assume we store user info in localStorage too
            const storedUser = localStorage.getItem('sentry_user');
            if (storedUser) {
                this.user = JSON.parse(storedUser);
            }
        } catch (e) {
            this.logout();
        } finally {
            this.initialized = true;
        }
    }

    login(token: string, user: { username: string; role: string }) {
        this.token = token;
        this.user = user;
        if (browser) {
            localStorage.setItem('sentry_token', token);
            localStorage.setItem('sentry_user', JSON.stringify(user));
        }
    }

    logout() {
        this.token = null;
        this.user = null;
        if (browser) {
            localStorage.removeItem('sentry_token');
            localStorage.removeItem('sentry_user');
            goto('/login');
        }
    }

    isAuthenticated() {
        return !!this.token;
    }
}

export const auth = new AuthState();
