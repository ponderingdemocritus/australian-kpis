const icon = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect width="64" height="64" rx="12" fill="#0f172a" />
  <path d="M13 43h38M18 37l8-9 7 5 12-15" fill="none" stroke="#38bdf8" stroke-linecap="round" stroke-linejoin="round" stroke-width="5" />
  <circle cx="18" cy="37" r="3" fill="#f8fafc" />
  <circle cx="26" cy="28" r="3" fill="#f8fafc" />
  <circle cx="33" cy="33" r="3" fill="#f8fafc" />
  <circle cx="45" cy="18" r="3" fill="#f8fafc" />
</svg>`

export function GET() {
  return new Response(icon, {
    headers: {
      'content-type': 'image/svg+xml',
    },
  })
}
