import { MoreHorizontal, RefreshCw, X, Pencil, Trash2, Unplug, Cable } from 'lucide-react';
import { useState, useRef, useEffect } from 'react';
import { Chat } from '../../types/chat';
import { ConnectionStatusBadge } from '../connection/ConnectionStatusBadge';

interface ChatHeaderProps {
  chat: Chat;
  isConnecting: boolean;
  isConnected: boolean;
  onClearChat: () => void;
  onEditConnection: () => void;
  onShowCloseConfirm: () => void;
  onReconnect: () => void;
  setShowRefreshSchema: () => void;
}

export function ChatHeader({
  chat,
  isConnecting,
  isConnected,
  onClearChat,
  onEditConnection,
  onShowCloseConfirm,
  onReconnect,
  setShowRefreshSchema
}: ChatHeaderProps) {
  const [showMenu, setShowMenu] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setShowMenu(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  return (
    <div className="
      sticky 
      top-0 
      left-0 
      right-0 
      z-10 
      px-4 
      md:px-6 
      border-b 
      border-gray-800
      bg-gray-900
      flex 
      items-center 
      justify-between 
      h-16
    ">
      <div className="flex items-center gap-3">
        <h2 className="font-semibold text-white truncate max-w-[200px] md:max-w-[300px]">
          {chat.title || 'New Chat'}
        </h2>
        <ConnectionStatusBadge isConnecting={isConnecting} isConnected={isConnected} />
      </div>

      <div className="flex items-center space-x-2">
        <button
          onClick={() => setShowRefreshSchema()}
          className="
            p-2
            text-gray-300
            hover:text-white
            hover:bg-gray-800
            rounded-md
            transition-colors
          "
          title="Refresh Knowledge Base"
        >
          <RefreshCw className="h-5 w-5" />
        </button>

        <div className="relative" ref={menuRef}>
          <button
            onClick={() => setShowMenu(!showMenu)}
            className="
              p-2
              text-gray-300
              hover:text-white
              hover:bg-gray-800
              rounded-md
              transition-colors
            "
            title="More Options"
          >
            <MoreHorizontal className="h-5 w-5" />
          </button>

          {showMenu && (
            <div className="
              absolute 
              right-0 
              mt-1 
              w-48 
              bg-gray-900 
              border 
              border-gray-700 
              shadow-xl 
              rounded-md 
              overflow-hidden
              z-50
            ">
              <div className="py-1">
                <button
                  onClick={() => {
                    onEditConnection();
                    setShowMenu(false);
                  }}
                  className="
                    flex 
                    w-full 
                    items-center 
                    gap-2 
                    px-4 
                    py-2 
                    text-left 
                    text-sm 
                    text-white 
                    hover:bg-gray-800
                  "
                >
                  <Pencil className="h-4 w-4" />
                  Edit Connection
                </button>
                
                <button
                  onClick={() => {
                    onReconnect();
                    setShowMenu(false);
                  }}
                  className="
                    flex 
                    w-full 
                    items-center 
                    gap-2 
                    px-4 
                    py-2 
                    text-left 
                    text-sm 
                    text-white 
                    hover:bg-gray-800
                  "
                >
                  <Cable className="h-4 w-4" />
                  Reconnect
                </button>

                <button
                  onClick={() => {
                    onClearChat();
                    setShowMenu(false);
                  }}
                  className="
                    flex 
                    w-full 
                    items-center 
                    gap-2 
                    px-4 
                    py-2 
                    text-left 
                    text-sm 
                    text-white 
                    hover:bg-gray-800
                  "
                >
                  <Trash2 className="h-4 w-4" />
                  Clear Chat
                </button>

                <button
                  onClick={() => {
                    onShowCloseConfirm();
                    setShowMenu(false);
                  }}
                  className="
                    flex 
                    w-full 
                    items-center 
                    gap-2 
                    px-4 
                    py-2 
                    text-left 
                    text-sm 
                    text-red-400 
                    hover:bg-gray-800
                  "
                >
                  <Unplug className="h-4 w-4" />
                  Disconnect
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}