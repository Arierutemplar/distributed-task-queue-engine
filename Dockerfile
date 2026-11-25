FROM node:22-alpine AS dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM dependencies AS build
COPY tsconfig.json ./
COPY src ./src
RUN npx tsc --project tsconfig.json

FROM node:22-alpine AS production-dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

FROM redis:7-alpine AS redis-tools

FROM node:22-alpine AS operations
ENV NODE_ENV=production
WORKDIR /app
RUN apk add --no-cache openssl libgcc libstdc++ && mkdir -p /app/fixture-data && chown node:node /app/fixture-data
COPY --from=redis-tools /usr/local/bin/redis-server /usr/local/bin/redis-cli /usr/local/bin/redis-check-rdb /usr/local/bin/
COPY --from=production-dependencies --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --chown=node:node package.json ./
COPY --chown=node:node ops/reliability ./ops/reliability
USER node
CMD ["node", "dist/src/operations/backup-cli.js", "agent"]

FROM node:22-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app

COPY --from=production-dependencies --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --chown=node:node package.json ./

USER node
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/health/ready').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "dist/src/index.js"]
