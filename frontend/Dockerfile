FROM node:20-slim AS builder

WORKDIR /app

# Accept VITE_ vars as build arguments (injected by docker-compose build.args)
ARG VITE_API_KEY=""
ARG VITE_TELLER_APP_ID=""
ARG VITE_TELLER_ENVIRONMENT="sandbox"

# Make them available to the build process
ENV VITE_API_KEY=$VITE_API_KEY
ENV VITE_TELLER_APP_ID=$VITE_TELLER_APP_ID
ENV VITE_TELLER_ENVIRONMENT=$VITE_TELLER_ENVIRONMENT

# Install dependencies first (layer caching)
COPY package.json package-lock.json* ./
RUN npm ci || npm install

# Copy source and build
COPY . .
ENV DOCKER=true
RUN npm run build

# Production stage
FROM node:20-slim

WORKDIR /app

# Copy built output and production dependencies
COPY --from=builder /app/build ./build
COPY --from=builder /app/package.json .
COPY --from=builder /app/node_modules ./node_modules

EXPOSE 3000

ENV PORT=3000
ENV HOST=0.0.0.0
ENV ORIGIN=http://localhost:3000
ENV BACKEND_URL=http://backend:8000
ENV BODY_SIZE_LIMIT=16777216

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD node -e "fetch('http://localhost:3000').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "build"]
