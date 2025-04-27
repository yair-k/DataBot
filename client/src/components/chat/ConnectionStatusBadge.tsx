import React from 'react';

interface ConnectionStatusBadgeProps {
  isConnecting: boolean;
  isConnected: boolean;
}

export function ConnectionStatusBadge({ isConnecting, isConnected }: ConnectionStatusBadgeProps) {
  return (
    <div className={`
      px-2 
      py-0.5 
      rounded-full 
      text-xs 
      font-medium 
      ${isConnecting 
        ? 'bg-yellow-900/50 text-yellow-400 border border-yellow-700/50' 
        : isConnected 
          ? 'bg-green-900/50 text-green-400 border border-green-700/50' 
          : 'bg-gray-800 text-gray-400 border border-gray-700/50'
      }
    `}>
      {isConnecting ? 'Connecting...' : isConnected ? 'Connected' : 'Disconnected'}
    </div>
  );
}