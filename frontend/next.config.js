/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    return [
      {
        source: '/api/v1/:path*',
        destination: 'http://localhost:8000/api/v1/:path*',
      },
      {
        source: '/api/v2/:path*',
        destination: 'http://localhost:8001/api/v2/:path*',
      },
    ]
  },
}

module.exports = nextConfig
