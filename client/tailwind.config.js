/** @type {import('tailwindcss').Config} */
export default {
  mode: "jit",
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        // Core dark theme colors
        dark: {
          bg: {
            primary: '#1A1A1A',     // Main background
            secondary: '#252525',   // Secondary/card background
            tertiary: '#2A2A2A',    // Tertiary/hover elements
          },
          text: {
            primary: '#FFFFFF',     // Primary text
            secondary: '#CCCCCC',   // Secondary text
            tertiary: '#999999',    // Tertiary/muted text
          },
          border: {
            primary: '#333333',     // Primary border
            secondary: '#3A3A3A',   // Secondary border
          }
        },
        // Light theme remains as fallback
        light: {
          bg: {
            primary: '#FFFFFF',
            secondary: '#F5F5F5',
            tertiary: '#EEEEEE',
          },
          text: {
            primary: '#111111',
            secondary: '#333333',
            tertiary: '#666666',
          },
          border: {
            primary: '#DDDDDD',
            secondary: '#EEEEEE',
          }
        },
        // Accent colors
        accent: {
          blue: '#17B5E2',
          teal: '#22E5BB',
          blue_dark: '#1497BD',
          teal_dark: '#1DC6A1',
        },
        // Special "neo" colors
        neo: {
          yellow: '#FFDB58',
          border: '#C0DFC0',
          error: '#FF4D4F',
          success: '#52C41A',
          warning: '#FAAD14',
        },
        background: '#1A1A1A',  // Update old background color
        border: '#C0DFC0',      // Keep existing border color
      },
      fontFamily: {
        sans: ['Inter', 'Public Sans', 'sans-serif'],
        display: ['Space Grotesk', 'Inter', 'sans-serif'],  // Added display font
      },
      boxShadow: {
        'neo-dark': '3px 3px 0px rgba(23, 181, 226, 0.3)',
        'neo-dark-lg': '5px 5px 0px rgba(23, 181, 226, 0.3)',
        'neo-dark-xl': '8px 8px 0px rgba(23, 181, 226, 0.3)',
      },
      backgroundImage: {
        'gradient-radial': 'radial-gradient(var(--tw-gradient-stops))',
        'gradient-dark': 'linear-gradient(to right, #1A1A1A, #252525)',
      },
    },
  },
  plugins: [],
};
