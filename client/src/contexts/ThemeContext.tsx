import React, { createContext, useContext, useEffect } from 'react';

type ThemeContextType = {
  isDarkTheme: boolean;
};

const ThemeContext = createContext<ThemeContextType>({
  isDarkTheme: true, // Always dark theme
});

export const useTheme = () => useContext(ThemeContext);

export const ThemeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // Always use dark theme
  const isDarkTheme = true;

  // Apply dark mode class to document
  useEffect(() => {
    document.documentElement.classList.add('dark');
  }, []);

  // Provide theme context to all children
  return (
    <ThemeContext.Provider value={{ isDarkTheme }}>
      {children}
    </ThemeContext.Provider>
  );
};