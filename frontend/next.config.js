/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  swcMinify: true,

  // API proxy configuration - proxy /api/* to backend
  // Uses localhost for local dev, orchestrator for Docker
  async rewrites() {
    const backendUrl = process.env.BACKEND_URL || 'http://orchestrator:8000'
    return [
      {
        source: '/api/:path*',
        destination: `${backendUrl}/api/:path*`,
      },
    ]
  },

  // Environment variables
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001',
    NEXT_PUBLIC_WS_URL: process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8001',
  },

  // NOTE: 'standalone' output removed - was causing CSS loading issues in development
  // For Docker deployments, add: output: 'standalone' and copy static files manually
  // See: https://nextjs.org/docs/app/api-reference/config/next-config-js/output
}

module.exports = nextConfig
