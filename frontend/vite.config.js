import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig, loadEnv } from 'vite';
import { injectEnvMeta } from './vite-env-plugin.js';

export default defineConfig(({ mode }) => {
    // Load .env files so process.env.VITE_API_KEY is available to the plugin
    const env = loadEnv(mode, process.cwd(), '');
    Object.assign(process.env, env);

    return {
        plugins: [
            injectEnvMeta(),
            sveltekit()
        ],
        server: {
            proxy: {
                '/api': env.BACKEND_URL || 'http://localhost:8000'
            }
        }
    };
});