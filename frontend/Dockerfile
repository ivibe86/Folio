FROM node:20-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0 AS builder

WORKDIR /app

ARG DEPENDENCY_COOLDOWN_DAYS=7

# Install dependencies first (layer caching)
COPY package.json package-lock.json ./
RUN cutoff="$(DEPENDENCY_COOLDOWN_DAYS="${DEPENDENCY_COOLDOWN_DAYS}" node -e "const days = Number(process.env.DEPENDENCY_COOLDOWN_DAYS || 7); console.log(new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString())")" && \
    npm ci --before="$cutoff"

# Accept VITE_ vars as build arguments only after dependency install scripts run.
ARG VITE_API_KEY=""
ARG VITE_TELLER_APP_ID=""
ARG VITE_TELLER_ENVIRONMENT="sandbox"

# Make them available to the build process
ENV VITE_API_KEY=$VITE_API_KEY
ENV VITE_TELLER_APP_ID=$VITE_TELLER_APP_ID
ENV VITE_TELLER_ENVIRONMENT=$VITE_TELLER_ENVIRONMENT

# Copy source and build
COPY . .
ENV DOCKER=true
RUN npm run build

# Production stage
FROM node:20-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0

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
