import { browser } from '$app/environment';

const BASE_URL = '/v1';

export async function apiFetch(path: string, options: RequestInit = {}) {
    const token = browser ? localStorage.getItem('sentry_token') : null;

    const headers = {
        'Content-Type': 'application/json',
        ...options.headers,
        ...(token ? { Authorization: `Bearer ${token}` } : {})
    };

    const response = await fetch(`${BASE_URL}${path}`, {
        ...options,
        headers
    });

    if (response.status === 401) {
        if (browser) {
            localStorage.removeItem('sentry_token');
            window.location.href = '/login';
        }
        throw new Error('Unauthorized');
    }

    if (!response.ok) {
        const error = await response.json().catch(() => ({ message: 'An error occurred' }));
        throw new Error(error.message || response.statusText);
    }

    return response.json();
}
