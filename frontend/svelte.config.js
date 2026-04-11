import adapterAuto from '@sveltejs/adapter-auto';
import adapterNode from '@sveltejs/adapter-node';

const isDocker = process.env.DOCKER === 'true';

/** @type {import('@sveltejs/kit').Config} */
const config = {
    kit: {
        adapter: isDocker
            ? adapterNode({ out: 'build' })
            : adapterAuto()
    }
};

export default config;