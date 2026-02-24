FROM node:20-slim

RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates procps \
    libxss1 libgconf-2-4 libxrandr2 libasound2 \
    libpangocairo-1.0-0 libatk1.0-0 libcairo-gobject2 \
    libgtk-3-0 libgdk-pixbuf2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./

ENV NODE_OPTIONS="--max-old-space-size=2048"

RUN npm install --omit=dev && \
    npx playwright install-deps && \
    npx playwright install chromium

COPY . .

EXPOSE 3000

CMD ["npx", "pm2-runtime", "--max-restarts", "5", "index.js"]
