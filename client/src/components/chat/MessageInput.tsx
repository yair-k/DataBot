import { Send } from 'lucide-react';
import React, { FormEvent, useEffect, useRef, useState } from 'react';
import TextareaAutosize from 'react-textarea-autosize';
import toast from 'react-hot-toast';

interface MessageInputProps {
  isConnected: boolean;
  onSendMessage: (message: string) => void;
  isExpanded?: boolean;
  isDisabled?: boolean;
}

export function MessageInput({ isConnected, onSendMessage, isExpanded = true, isDisabled = false }: MessageInputProps) {
  const [message, setMessage] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    // Focus textarea when component mounts
    if (textareaRef.current) {
      textareaRef.current.focus();
    }
  }, []);

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();

    if (!isConnected) {
      toast.error('Please connect to a database first');
      return;
    }

    if (!message.trim()) {
      return;
    }

    onSendMessage(message.trim());
    setMessage('');
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  return (
    <div className={`
      fixed 
      bottom-0 
      left-0 
      right-0 
      border-t 
      border-gray-800
      bg-gray-900
      z-10
      transition-all 
      duration-300
      ${isExpanded ? 'md:ml-80' : 'md:ml-20'}
    `}>
      <form onSubmit={handleSubmit} className={`
        max-w-5xl 
        mx-auto 
        px-4 
        py-4
        md:px-8 
        lg:px-4
        xl:px-0
        transition-all 
        duration-300
        ${isExpanded
          ? 'md:ml-6 lg:ml-4 xl:mx-auto [@media(min-width:1920px)]:ml-[8.4rem]'
          : 'md:ml-[19rem] xl:mx-auto'
        }
      `}>
        <div className="
          flex 
          items-end 
          gap-3 
          rounded-lg 
          border 
          border-gray-700
          bg-gray-950
          px-4
          shadow-[2px_2px_0px_0px_rgba(23,181,226,0.3)]
          focus-within:ring-1
          focus-within:ring-blue-500
          focus-within:border-blue-500
        ">
          <TextareaAutosize
            ref={textareaRef}
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Talk to your database..."
            className="
              flex-1 
              min-h-12
              max-h-64 
              resize-none 
              border-0 
              bg-transparent
              py-3
              text-base
              text-white
              placeholder:text-gray-400
              focus:outline-none
              disabled:opacity-50
            "
            disabled={isDisabled}
          />
          <button
            type="submit"
            className="
              mb-3
              p-1.5
              rounded-md
              text-gray-300
              hover:text-white
              hover:bg-gray-800
              disabled:opacity-50
              disabled:pointer-events-none
            "
            disabled={!message.trim() || !isConnected || isDisabled}
          >
            <Send className="h-5 w-5" />
          </button>
        </div>
      </form>
    </div>
  );
}