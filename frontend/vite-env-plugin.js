/**
 * Vite plugin that injects VITE_API_KEY into app.html as a <meta> tag.
 * This makes the key available to the inline prefetch script that runs
 * before any ES modules are evaluated.
 */
export function injectEnvMeta() {
    return {
        name: 'inject-env-meta',
        transformIndexHtml(html) {
            const apiKey = process.env.VITE_API_KEY || '';
            return html.replace(
                '%Folio_API_KEY%',
                apiKey
            );
        }
    };
}