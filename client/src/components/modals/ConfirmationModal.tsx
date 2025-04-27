import { AlertTriangle, Loader2, X } from 'lucide-react';
import { useState } from 'react';
import { useTheme } from '../../contexts/ThemeContext';

interface ConfirmationModalProps {
  themeColor?: string;
  buttonText?: string;
  icon?: React.ReactNode;
  title: string;
  message: string;
  onConfirm: () => Promise<void> | void;
  onCancel: () => void;
  confirmText?: string;
  cancelText?: string;
  isDangerous?: boolean;
}

export default function ConfirmationModal({
  title,
  message,
  onConfirm,
  onCancel,
  themeColor = 'neo-error',
  icon = <AlertTriangle className="w-6 h-6 text-neo-error" />,
  buttonText,
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  isDangerous = false
}: ConfirmationModalProps) {
  const [isLoading, setIsLoading] = useState(false);
  const { isDarkTheme } = useTheme();
  
  return (
    <div className={`fixed inset-0 ${isDarkTheme ? 'bg-dark-bg-primary/70' : 'bg-light-bg-primary/70'} backdrop-blur-sm flex items-center justify-center p-4 z-50`}>
      <div className={`${isDarkTheme ? 'bg-dark-bg-secondary border-dark-border-primary shadow-neo-dark' : 'bg-light-bg-secondary border-light-border-primary shadow-neo-light'} border rounded-xl w-full max-w-md`}>
        {/* Header */}
        <div className={`flex justify-between items-center p-5 border-b ${isDarkTheme ? 'border-dark-border-primary' : 'border-light-border-primary'}`}>
          <div className="flex items-center gap-3">
            {icon}
            <h2 className={`text-xl font-display font-medium ${isDarkTheme ? 'text-dark-text-primary' : 'text-light-text-primary'}`}>{title}</h2>
          </div>
          <button
            onClick={onCancel}
            className={`rounded-lg p-1.5 transition-colors ${isDarkTheme ? 'hover:bg-dark-bg-tertiary' : 'hover:bg-light-bg-tertiary'}`}
          >
            <X className={`w-5 h-5 ${isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}`} />
          </button>
        </div>

        {/* Content */}
        <div className="p-5">
          <p className={`mb-6 ${isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}`}>{message}</p>

          <div className="flex gap-4">
            <button
              onClick={async () => {
                try {
                  setIsLoading(true);
                  const result = onConfirm();
                  // Handle both Promise and non-Promise returns
                  if (result instanceof Promise) {
                    await result;
                  }
                } catch (error) {
                  console.error('Error in confirmation action:', error);
                } finally {
                  setIsLoading(false);
                }
              }}
              className={`bg-${themeColor} text-white px-4 py-2 font-medium text-base 
                          rounded-lg border border-${themeColor} transition-all 
                          hover:bg-${themeColor}/90 flex-1 flex justify-center items-center`}
              disabled={isLoading}
            >
              {isLoading ? (
                <div className="flex items-center justify-center gap-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  <span>Processing...</span>
                </div>
              ) : (
                buttonText || confirmText
              )}
            </button>
            <button
              onClick={onCancel}
              className="neo-button-secondary flex-1"
              disabled={isLoading}
            >
              {cancelText}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}