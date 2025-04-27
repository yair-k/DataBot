import { AlertCircle, Boxes, KeyRound, Loader, Lock, UserRound } from 'lucide-react';
import React, { useState, useEffect } from 'react';
import { LoginFormData, SignupFormData } from '../../types/auth';
import analyticsService from '../../services/analyticsService';
import { useTheme } from '../../contexts/ThemeContext';

interface AuthFormProps {
  onLogin: (data: LoginFormData) => Promise<void>;
  onSignup: (data: SignupFormData) => Promise<void>;
}

interface FormErrors {
  userName?: string;
  password?: string;
  confirmPassword?: string;
  userSignupSecret?: string;
}

export default function AuthForm({ onLogin, onSignup }: AuthFormProps) {
  const [isLogin, setIsLogin] = useState(true);
  const [errors, setErrors] = useState<FormErrors>({});
  const [touched, setTouched] = useState<Record<string, boolean>>({});
  const [isLoading, setIsLoading] = useState(false);
  const [formData, setFormData] = useState<SignupFormData>({
    userName: '',
    password: '',
    confirmPassword: '',
    userSignupSecret: ''
  });
  const [formError, setFormError] = useState<string | null>(null);
  const [animateLogo, setAnimateLogo] = useState(false);
  const { isDarkTheme } = useTheme();
  const Environment = import.meta.env.VITE_ENVIRONMENT;
  
  // Start logo animation after component mounts
  useEffect(() => {
    setAnimateLogo(true);
    
    // Clear any form errors when switching between login/signup modes
    setFormError(null);
    setErrors({});
    setTouched({});
  }, [isLogin]);

  const validateUserName = (userName: string) => {
    if (!userName) return 'Username is required';
    if (userName.length < 3) return 'Username must be at least 3 characters';
    if (userName.includes(' ')) return 'Username cannot contain spaces';
    return '';
  };

  const validatePassword = (password: string) => {
    if (!password) return 'Password is required';
    if (password.length < 6) {
      return 'Password must be at least 6 characters';
    }
    return '';
  };

  const validateUserSignupSecret = (userSignupSecret: string) => {
    if (!userSignupSecret) return 'User signup secret is required';
    return '';
  };
  
  const validateForm = () => {
    const newErrors: FormErrors = {};

    const userNameError = validateUserName(formData.userName);
    if (userNameError) newErrors.userName = userNameError;

    const passwordError = validatePassword(formData.password);
    if (passwordError) newErrors.password = passwordError;

    if (!isLogin && Environment !== "DEVELOPMENT") {
      const userSignupSecretError = validateUserSignupSecret(formData.userSignupSecret);
      if (userSignupSecretError) newErrors.userSignupSecret = userSignupSecretError;
    }

    if (!isLogin && formData.password !== formData.confirmPassword) {
      newErrors.confirmPassword = 'Passwords do not match';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrors({});
    setFormError(null);

    // Mark all fields as touched for validation
    const allTouched = Object.keys(formData).reduce((acc, key) => {
      acc[key] = true;
      return acc;
    }, {} as Record<string, boolean>);
    setTouched(allTouched);

    if (!validateForm()) return;

    setIsLoading(true);
    try {
      if (isLogin) {
        const { userName, password } = formData;
        await onLogin({ userName, password });
        
        // Note: User ID will be set in the App.tsx login handler
        // This is just a fallback event to track login attempt
        analyticsService.trackEvent('login_attempt', { username: userName });
      } else {
        await onSignup(formData);
        
        // Note: User ID will be set in the App.tsx signup handler
        // This is just a fallback event to track signup attempt
        analyticsService.trackEvent('signup_attempt', { username: formData.userName });
      }
    } catch (error: any) {
      setFormError(error.message || 'An unexpected error occurred. Please try again.');
      
      // Track failed login/signup
      analyticsService.trackEvent(
        isLogin ? 'login_error' : 'signup_error', 
        { error: error.message }
      );
    } finally {
      setIsLoading(false);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    
    // Prevent spaces in username
    if (name === 'userName' && value.includes(' ')) {
      // Remove spaces from the input
      const valueWithoutSpaces = value.replace(/\s/g, '');
      setFormData(prev => ({
        ...prev,
        [name]: valueWithoutSpaces
      }));
      
      if (touched[name]) {
        const error = validateUserName(valueWithoutSpaces);
        setErrors(prev => ({ ...prev, userName: error }));
      }
      return;
    }
    
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));

    if (touched[name]) {
      if (name === 'userName') {
        const trimmedValue = value.trim();
        const error = validateUserName(trimmedValue);
        setErrors(prev => ({ ...prev, userName: error }));
      } else if (name === 'password') {
        const error = validatePassword(value);
        setErrors(prev => ({ ...prev, password: error }));
        
        // Also validate confirmPassword if it's been touched
        if (touched['confirmPassword']) {
          setErrors(prev => ({
            ...prev,
            confirmPassword: value !== formData.confirmPassword ? 'Passwords do not match' : ''
          }));
        }
      } else if (name === 'confirmPassword') {
        setErrors(prev => ({
          ...prev,
          confirmPassword: value !== formData.password ? 'Passwords do not match' : ''
        }));
      } else if (name === 'userSignupSecret') {
        const error = validateUserSignupSecret(value);
        setErrors(prev => ({ ...prev, userSignupSecret: error }));
      }
    }
  };

  const handleBlur = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setTouched(prev => ({ ...prev, [name]: true }));

    if (name === 'userName') {
      // Trim spaces on blur for username
      const trimmedValue = value.trim();
      
      // Update the form data with trimmed value
      if (trimmedValue !== value) {
        setFormData(prev => ({
          ...prev,
          [name]: trimmedValue
        }));
      }
      
      const error = validateUserName(trimmedValue);
      setErrors(prev => ({ ...prev, userName: error }));
    } else if (name === 'password') {
      const error = validatePassword(value);
      setErrors(prev => ({ ...prev, password: error }));
    } else if (name === 'confirmPassword') {
      setErrors(prev => ({
        ...prev,
        confirmPassword: value !== formData.password ? 'Passwords do not match' : ''
      }));
    } else if (name === 'userSignupSecret') {
      const error = validateUserSignupSecret(value);
      setErrors(prev => ({ ...prev, userSignupSecret: error }));
    }
  };

  // Prevent spaces in username input
  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.currentTarget.name === 'userName' && e.key === ' ') {
      e.preventDefault();
    }
  };

  return (
    <div className="min-h-screen auth-bg flex items-center justify-center p-4">
      <div className="w-full max-w-md auth-card p-6 md:p-8 rounded-2xl shadow-lg">
        <div className="text-center mb-8">
          <div className={`flex items-center justify-center mb-4 ${animateLogo ? 'float-animation' : ''}`}>
            <div className="p-3 rounded-full bg-accent-blue/10">
              <Boxes className="w-12 h-12 text-accent-blue" />
            </div>
          </div>
          <h1 className="text-3xl font-bold mb-2 bg-gradient-to-r from-accent-blue to-accent-teal bg-clip-text text-transparent">
            DataBot
          </h1>
          <p className="text-light-text-secondary dark:text-dark-text-secondary">
            {isLogin ? 'Welcome back to DataBot!' : 'Create your account to start using DataBot'}
          </p>
        </div>

        {formError && (
          <div className="mb-6 p-4 bg-red-50 dark:bg-red-900/20 border-2 border-red-500 rounded-lg animate-[shake_0.5s_ease-in-out]">
            <div className="flex items-center gap-2 text-red-600 dark:text-red-400">
              <AlertCircle className="w-5 h-5" />
              <p className="font-medium">{formError}</p>
            </div>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-6">
          <div className="space-y-1">
            <div className="relative">
              <UserRound className="absolute left-4 top-1/2 transform -translate-y-1/2 text-light-text-tertiary dark:text-dark-text-tertiary" />
              <input
                type="text"
                name="userName"
                placeholder=""
                value={formData.userName}
                onChange={handleChange}
                onBlur={handleBlur}
                onKeyDown={handleKeyDown}
                className={`neo-input pl-12 w-full input-animated ${errors.userName && touched.userName ? 'border-neo-error' : ''}`}
                required
                autoComplete="username"
              />
              <span className="input-label">Username</span>
            </div>
            {errors.userName && touched.userName && (
              <div className="flex items-center gap-1 mt-1 text-neo-error text-sm animate-[fadeIn_0.3s_ease-in-out]">
                <AlertCircle className="w-4 h-4" />
                <span>{errors.userName}</span>
              </div>
            )}
          </div>

          {!isLogin && Environment !== "DEVELOPMENT" && (
            <div className="space-y-1">
              <div className="relative">
                <Lock className="absolute left-4 top-1/2 transform -translate-y-1/2 text-light-text-tertiary dark:text-dark-text-tertiary" />
                <input
                  type="text"
                  name="userSignupSecret"
                  placeholder=""
                  value={formData.userSignupSecret}
                  onChange={handleChange}
                  onBlur={handleBlur}
                  className={`neo-input pl-12 w-full input-animated ${errors.userSignupSecret && touched.userSignupSecret ? 'border-neo-error' : ''}`}
                  required
                />
                <span className="input-label">User Signup Secret</span>
              </div>
              <p className="text-light-text-tertiary dark:text-dark-text-tertiary text-sm mt-1 ml-1">
                Required to signup. Ask the admin for this secret.
              </p>
              {errors.userSignupSecret && touched.userSignupSecret && (
                <div className="flex items-center gap-1 mt-1 text-neo-error text-sm animate-[fadeIn_0.3s_ease-in-out]">
                  <AlertCircle className="w-4 h-4" />
                  <span>{errors.userSignupSecret}</span>
                </div>
              )}
            </div>
          )}

          <div className="space-y-1">
            <div className="relative">
              <KeyRound className="absolute left-4 top-1/2 transform -translate-y-1/2 text-light-text-tertiary dark:text-dark-text-tertiary" />
              <input
                type="password"
                name="password"
                placeholder=""
                value={formData.password}
                onChange={handleChange}
                onBlur={handleBlur}
                className={`neo-input pl-12 w-full input-animated ${errors.password && touched.password ? 'border-neo-error' : ''}`}
                required
                autoComplete={isLogin ? "current-password" : "new-password"}
              />
              <span className="input-label">Password</span>
            </div>
            {errors.password && touched.password && (
              <div className="flex items-center gap-1 mt-1 text-neo-error text-sm animate-[fadeIn_0.3s_ease-in-out]">
                <AlertCircle className="w-4 h-4" />
                <span>{errors.password}</span>
              </div>
            )}
            {!errors.password && formData.password && touched.password && (
              <div className="flex items-center gap-1 mt-1 text-green-600 dark:text-green-400 text-sm animate-[fadeIn_0.3s_ease-in-out]">
                <span>Password strength: Good</span>
              </div>
            )}
          </div>

          {!isLogin && (
            <div className="space-y-1">
              <div className="relative">
                <KeyRound className="absolute left-4 top-1/2 transform -translate-y-1/2 text-light-text-tertiary dark:text-dark-text-tertiary" />
                <input
                  type="password"
                  name="confirmPassword"
                  placeholder=""
                  value={formData.confirmPassword}
                  onChange={handleChange}
                  onBlur={handleBlur}
                  className={`neo-input pl-12 w-full input-animated ${errors.confirmPassword && touched.confirmPassword ? 'border-neo-error' : ''}`}
                  required
                  autoComplete="new-password"
                />
                <span className="input-label">Confirm Password</span>
              </div>
              {errors.confirmPassword && touched.confirmPassword && (
                <div className="flex items-center gap-1 mt-1 text-neo-error text-sm animate-[fadeIn_0.3s_ease-in-out]">
                  <AlertCircle className="w-4 h-4" />
                  <span>{errors.confirmPassword}</span>
                </div>
              )}
            </div>
          )}

          <button
            type="submit"
            className="neo-button w-full relative overflow-hidden group transition-all duration-200"
            disabled={isLoading}
          >
            <span className="relative z-10">
              {isLoading ? (
                <div className="flex items-center justify-center">
                  <Loader className="w-4 h-4 animate-spin mr-2" />
                  {isLogin ? 'Logging in...' : 'Signing up...'}
                </div>
              ) : (
                <span>{isLogin ? 'Login' : 'Sign Up'}</span>
              )}
            </span>
            <span className="absolute inset-0 bg-gradient-to-r from-accent-blue to-accent-teal transform scale-x-0 group-hover:scale-x-100 transition-transform origin-left duration-300"></span>
          </button>
          
          <div className="relative my-4 text-center">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-light-border-primary dark:border-dark-border-primary"></div>
            </div>
            <div className="relative flex justify-center">
              <span className="px-2 bg-light-bg-secondary dark:bg-dark-bg-secondary text-light-text-tertiary dark:text-dark-text-tertiary text-sm">
                {isLogin ? 'New to DataBot?' : 'Already have an account?'}
              </span>
            </div>
          </div>
          
          <button
            type="button"
            onClick={() => {
              setIsLogin(!isLogin);
              setFormData({
                userName: '',
                password: '',
                confirmPassword: '',
                userSignupSecret: ''
              });
            }}
            className="neo-button-secondary w-full transition-all duration-200"
            disabled={isLoading}
          >
            {isLogin ? 'Create Account' : 'Login to Your Account'}
          </button>
        </form>
      </div>
    </div>
  );
}