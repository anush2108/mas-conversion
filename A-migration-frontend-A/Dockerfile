#frontend/Dockerfile
FROM node:20
WORKDIR /app
# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install
# Copy frontend source code
COPY . .
# Build frontend
RUN npm run build
# Expose development server port
EXPOSE 3000
# Start React development server
CMD ["npm", "start"]