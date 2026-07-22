/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#0b1120",
        panel: "#151d2e",
        panel2: "#1e293b",
        edge: "#2a3852",
        sky: "#38bdf8",
        grass: "#22c55e",
      },
    },
  },
  plugins: [],
};
