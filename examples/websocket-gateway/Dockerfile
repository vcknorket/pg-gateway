# ---- Base Stage ----
# Use a specific Node.js version for reproducibility
FROM node:18-slim as base
WORKDIR /app

# ---- Dependencies Stage ----
# This stage is dedicated to installing all dependencies, including dev dependencies
# needed for building the TypeScript code.
FROM base as dependencies
# Copy all package.json files to leverage Docker layer caching.
# NPM needs all of them to correctly install workspace dependencies.
COPY package.json package-lock.json ./
COPY packages/pg-gateway/package.json ./packages/pg-gateway/
COPY examples/websocket-gateway/package.json ./examples/websocket-gateway/

# Install all dependencies
RUN npm install

# ---- Builder Stage ----
# This stage builds the TypeScript source code into JavaScript.
FROM dependencies as builder
# Copy the rest of the source code
COPY . .
# Build the core pg-gateway package
RUN npm run build --prefix packages/pg-gateway
# Build the websocket-gateway example itself
RUN npm run build --prefix examples/websocket-gateway

# ---- Production Stage ----
# This is the final, small, production-ready image.
FROM base as production
# Copy only the necessary production dependencies from the 'dependencies' stage.
# This avoids copying over dev dependencies.
COPY --from=dependencies /app/node_modules ./node_modules
COPY --from=dependencies /app/package.json ./
COPY --from=dependencies /app/packages/pg-gateway/package.json ./packages/pg-gateway/
COPY --from=dependencies /app/examples/websocket-gateway/package.json ./examples/websocket-gateway/

# Copy the compiled JavaScript code from the 'builder' stage.
COPY --from=builder /app/packages/pg-gateway/dist ./packages/pg-gateway/dist
COPY --from=builder /app/examples/websocket-gateway/dist ./examples/websocket-gateway/dist

# Expose the PostgreSQL port
EXPOSE 5432

# Set environment variables for the gateway (can be overridden at runtime)
ENV PG_PORT=5432
ENV WORKER_URL=ws://worker-service:8080

# The command to run the gateway application
CMD ["node", "examples/websocket-gateway/dist/index.js"]
