FROM node:20-slim

RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./

ENV NODE_OPTIONS="--max-old-space-size=2048"

RUN npm install --omit=dev && \
    npx playwright install-deps && \
    npx playwright install chromium && \
    rm -rf /var/lib/apt/lists/* /tmp/*

COPY . .

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3000/health || exit 1

CMD ["./node_modules/.bin/pm2-runtime", "--max-restarts", "5", "index.js"]
