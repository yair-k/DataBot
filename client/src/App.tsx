import axios from 'axios';
import { EventSourcePolyfill } from 'event-source-polyfill';
import { Boxes } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import toast, { Toaster } from 'react-hot-toast';
import { BrowserRouter as Router, Routes, Route, Navigate, useNavigate, useLocation } from 'react-router-dom';
import AuthForm from './components/auth/AuthForm';
import ChatWindow from './components/chat/ChatWindow';
import { Message, QueryResult, LoadingStep } from './components/chat/types';
import SuccessBanner from './components/common/SuccessBanner';
import Sidebar from './components/dashboard/Sidebar';
import ConnectionModal from './components/modals/ConnectionModal';
import { StreamProvider, useStream } from './contexts/StreamContext';
import { UserProvider, useUser } from './contexts/UserContext';
import { ThemeProvider } from './contexts/ThemeContext';
import authService from './services/authService';
import './services/axiosConfig';
import chatService from './services/chatService';
import analyticsService from './services/analyticsService';
import { LoginFormData, SignupFormData } from './types/auth';
import { Chat, ChatSettings, ChatsResponse, Connection } from './types/chat';
import { SendMessageResponse } from './types/messages';
import { StreamResponse } from './types/stream';
import WelcomeSection from './components/app/WelcomeSection';
import LoadingComponent from './components/app/Loading';

function AppContent() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [showConnectionModal, setShowConnectionModal] = useState(false);
  const [isEditingConnection, setIsEditingConnection] = useState(false);
  const [, setShowSelectTablesModal] = useState(false);
  const [isSidebarExpanded, setIsSidebarExpanded] = useState(true);
  const [selectedConnection, setSelectedConnection] = useState<Chat>();
  const [messages, setMessages] = useState<Message[]>([]);
  const [authError, setAuthError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [chats, setChats] = useState<Chat[]>([]);
  const [isLoadingChats, setIsLoadingChats] = useState(false);
  const [connectionStatuses, setConnectionStatuses] = useState<Record<string, boolean>>({});
  const [eventSource, setEventSource] = useState<EventSourcePolyfill | null>(null);
  const { streamId, setStreamId, generateStreamId } = useStream();
  const [isMessageSending, setIsMessageSending] = useState(false);
  const [temporaryMessage, setTemporaryMessage] = useState<Message | null>(null);
  const { user, setUser } = useUser();
  const [refreshSchemaController, setRefreshSchemaController] = useState<AbortController | null>(null);
  const [isSSEReconnecting, setIsSSEReconnecting] = useState(false);
  const [newlyCreatedChat, setNewlyCreatedChat] = useState<Chat | null>(null);
  
  // Debug useEffect for isSSEReconnecting state changes
  useEffect(() => {
  }, [isSSEReconnecting]);
  
  // Check auth status on mount
  useEffect(() => {
    checkAuth();
  }, []);

  // First, update the toast configurations
  const toastStyle = {
    style: {
      background: '#000',
      color: '#fff',
      border: '4px solid #000',
      borderRadius: '12px',
      boxShadow: '4px 4px 0px 0px rgba(0,0,0,1)',
      padding: '12px 24px',
      fontSize: '16px',
      fontWeight: '500',
      zIndex: 9999,
    },
  } as const;


  const errorToast = {
    style: {
      ...toastStyle.style,
      background: '#ff4444',  // Red background for errors
      border: '4px solid #cc0000',
      color: '#fff',
      fontWeight: 'bold',
    },
    duration: 4000,
    icon: '⚠️',
  };


  const checkAuth = async () => {
    try {
      console.log("Starting auth check...");
      const response = await authService.getUser();
      console.log("Auth check result:", response);
      setIsAuthenticated(response.success);
      
      if (response.success && response.data) {
        const userData = {
          id: response.data.id,
          username: response.data.username,
          created_at: response.data.created_at,
        };
        
        setUser(userData);
        
        // Set user identity in analytics
        try {
          analyticsService.identifyUser(
            userData.id,
            userData.username,
            userData.created_at
          );
          console.log('User identified in analytics');
        } catch (error) {
          console.error('Failed to identify user in analytics:', error);
        }
      }
      
      setAuthError(null);
    } catch (error: any) {
      console.error('Auth check failed:', error);
      setIsAuthenticated(false);
      setAuthError(error.message);
      toast.error(error.message, errorToast);
    } finally {
      setIsLoading(false);
    }
  };

  // Add useEffect debug
  useEffect(() => {
    console.log("Auth state changed:", isAuthenticated);
  }, [isAuthenticated]);

  // Update useEffect to handle auth errors
  useEffect(() => {
    if (authError) {
      toast.error(authError, errorToast);
      setAuthError(null);
    }
  }, [authError]);

  // Load chats when authenticated
  useEffect(() => {
    const loadChats = async () => {
      if (!isAuthenticated) return;

      setIsLoadingChats(true);
      try {
        const response = await axios.get<ChatsResponse>(`${import.meta.env.VITE_API_URL}/chats`, {
          withCredentials: true,
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('token')}`,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          }
        });
        console.log("Loaded chats:", response.data);
        if (response.data?.data?.chats) {
          setChats(response.data.data.chats);
        }
      } catch (error) {
        console.error("Failed to load chats:", error);
      } finally {
        setIsLoadingChats(false);
      }
    };

    loadChats();
  }, [isAuthenticated]);

  const handleLogin = async (data: LoginFormData) => {
    try {
      const response = await authService.login(data);
      if (response.success) {
        const userData = {
          id: response.data.user.id,
          username: response.data.user.username,
          created_at: response.data.user.created_at
        };
        
        setUser(userData);
        setIsAuthenticated(true);
        setSuccessMessage(`Welcome back, ${userData.username}!`);
        
        // Track login in analytics
        try {
          analyticsService.trackLogin(userData.id, userData.username);
          analyticsService.identifyUser(
            userData.id,
            userData.username,
            userData.created_at
          );
          console.log('Login tracked in analytics');
        } catch (error) {
          console.error('Failed to track login in analytics:', error);
        }
      }
    } catch (error: any) {
      console.error("Login error:", error);
      throw error;
    }
  };

  const handleSignup = async (data: SignupFormData) => {
    try {
      const response = await authService.signup(data);
      console.log("handleSignup response", response);
      
      const userData = {
        id: response.data.user.id,
        username: response.data.user.username,
        created_at: response.data.user.created_at
      };
      
      setIsAuthenticated(true);
      setUser(userData);
      setSuccessMessage(`Welcome to DataBot, ${userData.username}!`);
      
      // Track signup in analytics
      try {
        analyticsService.trackSignup(userData.id, userData.username);
        analyticsService.identifyUser(
          userData.id,
          userData.username,
          userData.created_at
        );
        console.log('Signup tracked in analytics');
      } catch (error) {
        console.error('Failed to track signup in analytics:', error);
      }
    } catch (error: any) {
      console.error("Signup error:", error);
      throw error;
    }
  };

  const handleAddConnection = async (connection: Connection, settings: ChatSettings): Promise<{ success: boolean, error?: string, chatId?: string }> => {
    try {
      const newChat = await chatService.createChat(connection, settings);
      setChats(prev => [...prev, newChat]);
      setSuccessMessage('Connection added successfully!');
      
      
      // Return the newly created chat ID so ConnectionModal can fetch tables
      return { success: true, chatId: newChat.id };
    } catch (error: any) {
      console.error('Failed to add connection:', error);
      toast.error(error.message, errorToast);
      return { success: false, error: error.message };
    }
  };

  const handleLogout = async () => {
    try {
      await authService.logout();
      setUser(null);
      setSuccessMessage('You\'ve been logged out!');
      setIsAuthenticated(false);
      setSelectedConnection(undefined);
      setMessages([]);
    } catch (error: any) {
      console.error('Logout failed:', error);
      setIsAuthenticated(false);
    }
  };

  const handleClearChat = async () => {
    // Make API call to clear chat
    try {
      await axios.delete(`${import.meta.env.VITE_API_URL}/chats/${selectedConnection?.id}/messages`, {
        withCredentials: true,
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('token')}`
        }
      });
      setMessages([]);
    } catch (error: any) {
      console.error('Failed to clear chat:', error);
      toast.error(error.message, errorToast);
    }
  };

  const handleConnectionStatusChange = useCallback((chatId: string, isConnected: boolean, from: string) => {
    console.log('Connection status changed:', { chatId, isConnected, from });
    if (chatId && typeof isConnected === 'boolean') { // Strict type check
      setConnectionStatuses(prev => ({
        ...prev,
        [chatId]: isConnected
      }));
    }
  }, []);

  const handleCloseConnection = useCallback(async () => {
    if (eventSource) {
      console.log('Closing SSE connection');
      eventSource.close();
      setEventSource(null);
      // Disconnect from the connection
      await chatService.disconnectFromConnection(selectedConnection?.id || '', streamId || '');
      // Update connection status
      handleConnectionStatusChange(selectedConnection?.id || '', false, 'app-close-connection');
    }

    // Clear messages
    setMessages([]);

    // Clear connection status
    if (selectedConnection) {
      handleConnectionStatusChange(selectedConnection.id, false, 'app-close-connection');
    }

    // Clear messages and selected connection
    setMessages([]);
    setSelectedConnection(undefined);
  }, [eventSource, selectedConnection, handleConnectionStatusChange]);

  const handleDeleteConnection = async (id: string) => {
    try {
      // Remove from UI state
      setChats(prev => prev.filter(chat => chat.id !== id));

      // If the deleted chat was selected, clear the selection
      if (selectedConnection?.id === id) {
        setSelectedConnection(undefined);
        setMessages([]); // Clear messages if showing deleted chat
      }

      if (chats.length === 0) {
        setSelectedConnection(undefined);
      }
      // Show success message
      setSuccessMessage('Connection deleted successfully');
    } catch (error: any) {
      console.error('Failed to delete connection:', error);
      toast.error(error.message, errorToast);
    }
  };

  const handleEditConnection = async (id: string, data: Connection, settings: ChatSettings): Promise<{ success: boolean; error?: string }> => {
    let loadingToastId: string | undefined;
    loadingToastId = toast.loading('Updating connection...', {
      style: {
        background: '#000',
        color: '#fff',
        borderRadius: '12px',
        border: '4px solid #000',
      },
    });

    try {
      // Check if connection details have changed
      const credentialsChanged = selectedConnection &&
        (selectedConnection.connection.database !== data.database ||
        selectedConnection.connection.host !== data.host ||
        selectedConnection.connection.port !== data.port ||
        selectedConnection.connection.username !== data.username);

      // Update the connection
      const response = await chatService.editChat(id, data, settings);
      console.log("handleEditConnection response", response);

      if (response) {
        // Update local state
        setChats(prev => prev.map(chat => 
          chat.id === id ? response : chat
        ));
        
        if (selectedConnection?.id === id) {
          setSelectedConnection(response);
        }

        // If credentials changed and we have a valid streamId, we need to handle reconnection
        if (credentialsChanged && streamId) {
          try {
            // First disconnect the current connection
            await chatService.disconnectFromConnection(id, streamId);
            
            // Clear any existing tables cache for this chat
            delete chatService.tablesCache?.[id];

            // Wait a bit before reconnecting to ensure disconnection is complete
            await new Promise(resolve => setTimeout(resolve, 1000));

            // Reconnect with new connection details
            await chatService.connectToConnection(id, streamId);

            // Update connection status
            handleConnectionStatusChange(id, true, 'edit-connection');
          } catch (error) {
            console.error('Error during reconnection:', error);
            toast.error('Failed to reconnect to database. Please try reconnecting manually.', {
              style: {
                background: '#000',
                color: '#fff',
                borderRadius: '12px',
                border: '4px solid #f00',
              },
            });
          }
        } else if (credentialsChanged) {
          // If credentials changed but no streamId, show a notification
          toast.error('Connection details updated. Please reconnect to the database.', {
            style: {
              background: '#000',
              color: '#fff',
              borderRadius: '12px',
              border: '4px solid #ff9800',
            },
          });
          // Set connection status to false since we couldn't reconnect
          handleConnectionStatusChange(id, false, 'edit-connection-no-stream');
        }

        toast.success('Connection updated successfully!', {
          style: {
            background: '#000',
            color: '#fff',
            borderRadius: '12px',
            border: '4px solid #000',
          },
        });

        if (loadingToastId) {
          toast.dismiss(loadingToastId);
        }

        return { success: true };
      }

      if (loadingToastId) {
        toast.dismiss(loadingToastId);
      }

      return { success: false, error: 'Failed to update connection' };
    } catch (error: any) {
      console.error('Failed to update connection:', error);
      
      if (loadingToastId) {
        toast.dismiss(loadingToastId);
      }

      return { success: false, error: error.message || 'Failed to update connection' };
    }
  };

  useEffect(() => {
    if (!selectedConnection) {
      setConnectionStatuses({});
    }
  }, [selectedConnection]);

  const handleSelectConnection = useCallback(async (id: string) => {
    console.log('handleSelectConnection happened in app.tsx', { id });
    const currentConnection = selectedConnection;
    const connection = chats.find(c => c.id === id);
    if (connection) {
      console.log('connection found', { connection });
      setSelectedConnection(connection);

      // Check if the connection is already connected
      const isConnected = connectionStatuses[id];
      if (isConnected) {
        handleConnectionStatusChange(id, true, 'app-select-connection');
      } else {
        // Make api call to to check connection status
        const connectionStatus = await chatService.checkConnectionStatus(id);
        console.log('connectionStatus in handleSelectConnection', { connectionStatus });
        if (connectionStatus) {
          handleConnectionStatusChange(id, true, 'app-select-connection');
        } else {
          console.log('connectionStatus is false, connecting to the connection');
          // Make api call to connect to the connection
          await chatService.connectToConnection(id, streamId || '');
          handleConnectionStatusChange(id, true, 'app-select-connection');
        }
      }

      if (currentConnection?.id != connection?.id) {
        // Check eventsource state
        console.log('eventSource?.readyState', eventSource?.readyState);
        if (eventSource?.readyState === EventSource.OPEN) {
          console.log('eventSource is open');
        }
        console.log('Setting up new connection');
        await setupSSEConnection(id);
      }
    }
  }, [chats, connectionStatuses, handleConnectionStatusChange]);

  // Update setupSSEConnection to include onclose
  const setupSSEConnection = useCallback(async (chatId: string): Promise<string> => {
    try {
      // Close existing SSE connection if any
      if (eventSource) {
        console.log('Closing existing SSE connection');
        eventSource.close();
        setEventSource(null);
      }

      // Generate new stream ID only if we don't have one
      let localStreamId = streamId;
      if (!localStreamId) {
        localStreamId = generateStreamId();
        setStreamId(localStreamId);
      }

      // Wait for old connection to fully close
      await new Promise(resolve => setTimeout(resolve, 100));

      console.log(`Setting up new SSE connection for chat ${chatId} with stream ${localStreamId}`);

      // Create and setup new SSE connection
      const sse = new EventSourcePolyfill(
        `${import.meta.env.VITE_API_URL}/chats/${chatId}/stream?stream_id=${localStreamId}`,
        {
          withCredentials: true,
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('token')}`
          }
        }
      );

      // Setup SSE event handlers
      sse.onopen = () => {
        console.log('SSE connection opened successfully');
        setIsSSEReconnecting(true);
        // The console.log below will still show the old value because setState is asynchronous
        // Use a timeout to allow the state to update before checking
        setTimeout(() => {
          console.log('SSE connection opened successfully -> isSSEReconnecting (after update)', true);
        }, 0);
      };

      sse.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          console.log('SSE message received:', data);

          if (data.event === 'db-connected') {
            handleConnectionStatusChange(chatId, true, 'app-sse-connection');
          } else if (data.event === 'db-disconnected') {
            handleConnectionStatusChange(chatId, false, 'app-sse-connection');
          }
        } catch (error) {
          console.error('Failed to parse SSE message:', error);
        }
      };

      sse.onerror = (e: any) => {
        console.error('SSE connection error:', e);
        // Here's check if the SSE connection is tried to be reconnected by SSE-open
        setTimeout(() => {
          console.log('SSE connection error -> isSSEReconnecting', isSSEReconnecting);
          // If false, that means the connection was not tried to be reconnected by SSE-open
          if (!isSSEReconnecting) {
            handleConnectionStatusChange(chatId, false, 'sse-error');
            // Don't close the connection on every error
            if (sse.readyState === EventSource.CLOSED) {
              setEventSource(null);
            }
          } else {
            // If true, that means the connection was tried to be reconnected by SSE-open
            console.log('SSE connection error -> isSSEReconnecting is true, making it false');
            setIsSSEReconnecting(false);
          }
        }, 100); // Increase timeout to ensure state has updated
      };

      setEventSource(sse);
      return localStreamId;

    } catch (error) {
      console.error('Failed to setup SSE connection:', error);
      toast.error('Failed to setup SSE connection', errorToast);
      throw error;
    }
  }, [eventSource, streamId, generateStreamId, handleConnectionStatusChange]);

  // Cleanup SSE on unmount or connection change
  useEffect(() => {
    return () => {
      if (eventSource) {
        eventSource.close();
        setEventSource(null);
      }
    };
  }, [eventSource]);

  // Refresh schema
  const handleRefreshSchema = async () => {
    try {
      const controller = new AbortController();
      setRefreshSchemaController(controller);
      console.log('handleRefreshSchema called');
      const response = await chatService.refreshSchema(selectedConnection?.id || '', controller);
      console.log('handleRefreshSchema response', response);
      if (response) {
        toast.success('Knowledge base refreshed successfully');
      } else {
        toast.error('Cancelled Knowledge Base Refresh');
      }
    } catch (error) {
      console.error('Failed to refresh knowledge base:', error);
      toast.error('Failed to refresh knowledge base ' + error);
    }
  };

  const handleCancelRefreshSchema = async () => {
    if (refreshSchemaController) {
      refreshSchemaController.abort();
      setRefreshSchemaController(null);
    }
  };

  const handleCancelStream = async () => {
    if (!selectedConnection?.id || !streamId) return;
    try {
      console.log('handleCancelStream -> streamId', streamId);
      await chatService.cancelStream(selectedConnection.id, streamId);

      // Remove temporary streaming message
      setMessages(prev => {
        return prev.filter(msg => !(msg.is_streaming && msg.id === 'temp'));
      });

      // Set isStreaming to false for all messages
      setMessages(prev => {
        return prev.map(msg => ({
          ...msg,
          is_streaming: false
        }));
      });

    } catch (error) {
      console.error('Failed to cancel stream:', error);
    }
  };

  // Add helper function for typing animation
  const animateTyping = async (text: string, messageId: string) => {
    // Reset content to empty before starting animation
    setMessages(prev => {
      return prev.map(msg => {
        if (msg.id === messageId) {
          return {
            ...msg,
            content: '',
            is_streaming: true,
            // Preserve action buttons and other properties
            action_buttons: msg.action_buttons
          };
        }
        return msg;
      });
    });

    // Animate content word by word with natural timing
    const words = text.split(' ');
    for (const word of words) {
      await new Promise(resolve => setTimeout(resolve, 15 + Math.random() * 15));
      setMessages(prev => {
        return prev.map(msg => {
          if (msg.id === messageId) {
            return {
              ...msg,
              content: msg.content + (msg.content ? ' ' : '') + word,
              // Preserve action buttons during animation
              action_buttons: msg.action_buttons
            };
          }
          return msg;
        });
      });
    }

    // Mark as no longer streaming when done
    setMessages(prev => {
      return prev.map(msg => {
        if (msg.id === messageId) {
          return {
            ...msg,
            is_streaming: false,
            // Preserve action buttons when animation is complete
            action_buttons: msg.action_buttons
          };
        }
        return msg;
      });
    });
  };

  // Add helper function for animating queries
  const animateQueryTyping = async (messageId: string, queries: QueryResult[]) => {
    if (!queries || queries.length === 0) return;

    // First ensure queries are initialized with empty strings
    setMessages(prev => {
      return prev.map(msg => {
        if (msg.id === messageId) {
          const initializedQueries = queries.map(q => ({
            ...q,
            query: ''
          }));
          return {
            ...msg,
            queries: initializedQueries,
            is_streaming: true,
            // Preserve action buttons during query initialization
            action_buttons: msg.action_buttons
          };
        }
        return msg;
      });
    });

    // Then animate each query
    for (const query of queries) {
      const queryWords = query.query.split(' ');
      for (const word of queryWords) {
        await new Promise(resolve => setTimeout(resolve, 20 + Math.random() * 10));
        setMessages(prev => {
          return prev.map(msg => {
            if (msg.id === messageId) {
              const updatedQueries = [...(msg.queries || [])];
              const queryIndex = updatedQueries.findIndex(q => q.id === query.id);
              if (queryIndex !== -1) {
                updatedQueries[queryIndex] = {
                  ...updatedQueries[queryIndex],
                  query: updatedQueries[queryIndex].query + (updatedQueries[queryIndex].query ? ' ' : '') + word
                };
              }
              return {
                ...msg,
                queries: updatedQueries,
                // Preserve action buttons during query animation
                action_buttons: msg.action_buttons
              };
            }
            return msg;
          });
        });
      }
    }

    // Mark as no longer streaming when done with all queries
    setMessages(prev => {
      return prev.map(msg => {
        if (msg.id === messageId) {
          return {
            ...msg,
            is_streaming: false,
            // Preserve action buttons when query animation is complete
            action_buttons: msg.action_buttons
          };
        }
        return msg;
      });
    });
  };

  const checkSSEConnection = async () => {
    console.log('checkSSEConnection -> eventSource?.readyState', eventSource?.readyState);
    if (eventSource?.readyState === EventSource.OPEN) {
      console.log('checkSSEConnection -> EventSource is open');
    } else {
      console.log('checkSSEConnection -> EventSource is not open');
      console.log('checkSSEConnection -> current stream id', streamId);
      await setupSSEConnection(selectedConnection?.id || '');
    }
    console.log('new stream id', streamId);
  };

  const handleSendMessage = async (content: string) => {
    if (!selectedConnection?.id || !streamId || isMessageSending) return;

    try {
      setIsMessageSending(true);
      console.log('handleSendMessage -> content', content);
      console.log('handleSendMessage -> streamId', streamId);

      // Check if the eventSource is open
      console.log('eventSource?.readyState', eventSource?.readyState);
      if (eventSource?.readyState === EventSource.OPEN) {
        console.log('EventSource is open');
      } else {
        console.log('EventSource is not open');
        console.log('current stream id', streamId);
        await setupSSEConnection(selectedConnection.id);
      }
      console.log('new stream id', streamId);

      // Wait for 100 ms for the eventSource to be open
      await new Promise(resolve => setTimeout(resolve, 100));
      const response = await chatService.sendMessage(selectedConnection.id, 'temp', streamId, content);

      // Update the chat updated_at field of the selected connection
      if (selectedConnection) {
        selectedConnection.updated_at = new Date().toISOString();
        chats.find(chat => chat.id === selectedConnection.id)!.updated_at = new Date().toISOString();
      }

      if (response.success) {
        const userMessage: Message = {
          id: response.data.id,
          type: 'user',
          content: response.data.content,
          is_loading: false,
          queries: [],
          is_streaming: false,
          created_at: new Date().toISOString()
        };

        // Add user message to the end
        setMessages(prev => [...prev, userMessage]);

        console.log('ai-response-step -> creating new temp message');
        const tempMsg: Message = {
          id: `temp`,
          type: 'assistant',
          content: '',
          queries: [],
          action_buttons: [], // Initialize with empty action buttons array
          is_loading: true,
          loading_steps: [{ text: 'DataBot is analyzing your request..', done: false }],
          is_streaming: true,
          created_at: new Date().toISOString()
        };

        // Add temp message to the end
        setMessages(prev => [...prev, tempMsg]);
        setTemporaryMessage(tempMsg);
      }
    } catch (error) {
      console.error('Failed to send message:', error);
      toast.error('Failed to send message', errorToast);
    } finally {
      setIsMessageSending(false);
    }
  };


  // Update SSE handling
  useEffect(() => {
    if (!eventSource) return;

    const handleSSEMessage = async function (this: EventSource, e: any) {
      try {
        console.log('handleSSEMessage -> msg', e);
        const response: StreamResponse = JSON.parse(e.data);
        console.log('handleSSEMessage -> response', response);

        switch (response.event) {
          case 'db-connected':
            console.log('db-connected -> response', response);
            if (selectedConnection) {
              handleConnectionStatusChange(selectedConnection.id, true, 'app-sse-connection');
            }

            break;
          case 'db-disconnected':
            console.log('db-disconnected -> response', response);
            if (selectedConnection) {
              handleConnectionStatusChange(selectedConnection.id, false, 'app-sse-connection');
            }
            break;
          case 'ai-response-step':
            // Set default of 500 ms delay for first step
            await new Promise(resolve => setTimeout(resolve, 500));

            if (!temporaryMessage ) {
              console.log('ai-response-step -> creating new temp message');
            } else {
              console.log('ai-response-step -> updating existing temp message');
              // Update the existing message with new step
              setMessages(prev => {
                // Find the streaming message
                const streamingMessage = prev.find(msg => msg.is_streaming);
                if (!streamingMessage) return prev;

                // No need to update the message if the step is DataBot is analyzing your request..
                if (streamingMessage.loading_steps && streamingMessage.loading_steps.length > 0 && response.data === 'DataBot is analyzing your request..') {
                  return prev;
                }

                // Create updated message with new step
                const updatedMessage = {
                  ...streamingMessage,
                  loading_steps: [
                    ...(streamingMessage.loading_steps || []).map((step: LoadingStep) => ({ ...step, done: true })),
                    { text: response.data, done: false }
                  ]
                };

                // Replace the streaming message in the array
                return prev.map(msg =>
                  msg.id === streamingMessage.id ? updatedMessage : msg
                );
              });
            }
            break;

          case 'ai-response':
            if (response.data) {
              console.log('ai-response -> response.data', response.data);

              // Check if this is a response to an edited message
              const isEditedResponse = response.data.user_message_id && 
                messages.some(msg => msg.id === response.data.user_message_id && msg.is_edited);
              
              console.log('ai-response -> isEditedResponse', isEditedResponse);
              console.log('ai-response -> user_message_id', response.data.user_message_id);
              
              // Find existing AI message that needs to be updated (for edited messages)
              const existingAiMessageIndex = isEditedResponse ? 
                messages.findIndex(msg => 
                  msg.type === 'assistant' && 
                  msg.user_message_id === response.data.user_message_id
                ) : -1;
              
              console.log('ai-response -> existingAiMessageIndex', existingAiMessageIndex);
              
              if (isEditedResponse && existingAiMessageIndex !== -1) {
                console.log('ai-response -> updating existing message at index', existingAiMessageIndex);
                
                // For edited messages, update the existing message with the new data
                const existingMessage = messages[existingAiMessageIndex];
                
                // First update the message with the new data but keep content empty for animation
                setMessages(prev => {
                  return prev.map((msg, index) => {
                    if (index === existingAiMessageIndex) {
                      return {
                        ...msg,
                        id: msg.id, // Keep the original ID
                        content: '', // Reset content to empty for animation
                        action_buttons: response.data.action_buttons, // Update action buttons from response
                        queries: response.data.queries?.map((q: QueryResult) => ({...q, query: ''})) || [], // Initialize queries with empty strings
                        is_loading: false,
                        loading_steps: [],
                        is_streaming: true,
                        user_message_id: response.data.user_message_id,
                        updated_at: new Date().toISOString(),
                        action_at: response.data.action_at
                      };
                    }
                    return msg;
                  });
                });
                
                // Animate content
                await animateTyping(response.data.content, existingMessage.id);
                
                // Animate queries
                if (response.data.queries && response.data.queries.length > 0) {
                  await animateQueryTyping(existingMessage.id, response.data.queries);
                }
                
                // Set final state - no longer streaming
                setMessages(prev => {
                  return prev.map((msg, index) => {
                    if (index === existingAiMessageIndex) {
                      return {
                        ...msg,
                        action_buttons: response.data.action_buttons, // Ensure action buttons are preserved
                        is_streaming: false,
                        updated_at: new Date().toISOString(),
                        action_at: response.data.action_at
                      };
                    }
                    return msg;
                  });
                });
              } else {
                // For new messages, create a new message
                // Create base message with empty content for animation
                const baseMessage: Message = {
                  id: response.data.id,
                  type: 'assistant' as const,
                  content: '',
                  action_buttons: response.data.action_buttons,
                  queries: response.data.queries?.map((q: QueryResult) => ({...q, query: ''})) || [],
                  is_loading: false,
                  loading_steps: [], // Clear loading steps for final message
                  is_streaming: true,
                  created_at: new Date().toISOString(),
                  user_message_id: response.data.user_message_id,
                  action_at: response.data.action_at
                };

                // Add the new message to the array
                setMessages(prev => {
                  const withoutTemp = prev.filter(msg => !msg.is_streaming);
                  console.log('ai-response -> withoutTemp', withoutTemp);
                  return [...withoutTemp, baseMessage];
                });

                // Animate content
                await animateTyping(response.data.content, response.data.id);
                
                // Animate queries
                if (response.data.queries && response.data.queries.length > 0) {
                  await animateQueryTyping(response.data.id, response.data.queries);
                }
                
                // Set final state - no longer streaming for new messages
                setMessages(prev => {
                  return prev.map(msg => {
                    if (msg.id === response.data.id) {
                      return {
                        ...msg,
                        is_streaming: false,
                        action_buttons: response.data.action_buttons, // Ensure action buttons are preserved
                        action_at: response.data.action_at,
                        updated_at: new Date().toISOString()
                      };
                    }
                    return msg;
                  });
                });
              }
            }
            setTemporaryMessage(null);
            break;

          case 'ai-response-error':
            // Show error message instead of temporary message
            setMessages(prev => {
              const withoutTemp = prev.filter(msg => !msg.is_streaming);
              return [{
                id: `error-${Date.now()}`,
                type: 'assistant',
                content: `${typeof response.data === 'object' ? response.data.error : response.data}`, // Handle both string and object errors
                queries: [],
                is_loading: false,
                loading_steps: [],
                is_streaming: false,
                created_at: new Date().toISOString()
              }, ...withoutTemp];
            });
            setTemporaryMessage(null);

            break;

          case 'response-cancelled':
            // Remove temporary streaming message
            setMessages(prev => {
              return prev.filter(msg => !(msg.is_streaming && msg.id === 'temp'));
            });

            const cancelMsg: Message = {
              id: `cancelled-${Date.now()}`,
              type: 'assistant',
              content: '',  // Start empty for animation
              queries: [],
              is_loading: false,
              loading_steps: [], // Clear loading steps
              is_streaming: false, // Set to false immediately
              created_at: new Date().toISOString()
            };

            // Add cancel message
            setMessages(prev => {
              const withoutTemp = prev.filter(msg => !msg.is_streaming);
              return [cancelMsg, ...withoutTemp];
            });

            // Animate cancel message
            await animateTyping(response.data, cancelMsg.id);

            // Clear temporary message state
            setTemporaryMessage(null);

            // Set streaming to false for all messages
            setMessages(prev =>
              prev.map(msg => ({
                ...msg,
                is_streaming: false
              }))
            );
            break;

        }
      } catch (error) {
        console.error('Failed to parse SSE message:', error);
      }
    };

    eventSource.onmessage = handleSSEMessage

    return () => {
      eventSource.onmessage = null;
    };
  }, [eventSource, temporaryMessage, selectedConnection?.id, streamId]);

  // Update the handleEditMessage function similarly
  const handleEditMessage = async (id: string, content: string) => {
    if (!selectedConnection?.id || !streamId || isMessageSending || content === '') return;

    try {
      console.log('handleEditMessage -> content', content);
      console.log('handleEditMessage -> streamId', streamId);

      if (eventSource?.readyState === EventSource.OPEN) {
        console.log('EventSource is open');
      } else {
        console.log('EventSource is not open');
        await setupSSEConnection(selectedConnection.id);
      }

      const response = await axios.patch<SendMessageResponse>(
        `${import.meta.env.VITE_API_URL}/chats/${selectedConnection.id}/messages/${id}`,
        {
          stream_id: streamId,
          content: content
        },
        {
          withCredentials: true,
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${localStorage.getItem('token')}`
          }
        }
      );

      if (response.data.success) {
        // Update the chat updated_at field of the selected connection
        if (selectedConnection) {
          selectedConnection.updated_at = new Date().toISOString();
          chats.find(chat => chat.id === selectedConnection.id)!.updated_at = new Date().toISOString();
        }

        // Set is_edited to true
        setMessages(prev => prev.map(msg => {
          if (msg.id === id) {
            return { ...msg, content: content, is_edited: true, updated_at: new Date().toISOString()};
          } 
          return msg;
        }));

        // Find the ai message where user_message_id is the id of the message
        console.log('handleEditMessage -> finding ai message for user message id', id);
        const aiMessage = messages.find(msg => {
          console.log('handleEditMessage -> finding ai message for user message id:', id, msg);
          return msg.user_message_id === id;
        });
        if(aiMessage) {
          // Update the ai message with the new content
          setMessages(prev => prev.map(msg => {
            if (msg.id === aiMessage.id) {
              return { 
                ...msg, 
                is_edited: true, 
                content:"", 
                queries: [], 
                action_buttons: [], // Reset action buttons for the edited message
                updated_at: new Date().toISOString(), 
                loading_steps: [{ text: 'DataBot is analyzing your request..', done: false }], 
                is_streaming: true 
              };
            } 
            return msg;
          }));
          setTemporaryMessage(messages.find(msg => msg.id === aiMessage.id) || null);
        } else {
          console.log('handleEditMessage -> aiMessage not found');
          const tempMsg: Message = {
            id: `temp`,
            type: 'assistant',
            content: '',
            queries: [],
            action_buttons: [], // Initialize with empty action buttons array
            is_loading: true,
            loading_steps: [{ text: 'DataBot is analyzing your request..', done: false }],
            is_streaming: true,
            created_at: new Date().toISOString()
          };
          setMessages(prev => [...prev, tempMsg]);
          setTemporaryMessage(tempMsg);
        }
      }
    } catch (error) {
      console.error('Failed to edit message:', error);
      toast.error('Failed to edit message', errorToast);
    }
  };

  // Add function to handle updating selected collections
  const handleUpdateSelectedCollections = async (chatId: string, selectedCollections: string): Promise<void> => {
    let loadingToast: string | null = null;
    
    try {
    
      // Always make the API call regardless of whether the selection has changed
      loadingToast = toast.loading('Updating selected tables...', {
        style: {
          background: '#000',
          color: '#fff',
          borderRadius: '12px',
          border: '4px solid #000',
        },
      });
      
      // Call the API to update the selected collections
      await chatService.updateSelectedCollections(chatId, selectedCollections);
      
      // Update the chat in the local state
      setChats(prev => prev.map(chat => 
        chat.id === chatId ? { ...chat, selected_collections: selectedCollections } : chat
      ));
      
      // If this is the selected connection, update it
      if (selectedConnection?.id === chatId) {
        setSelectedConnection(prev => prev ? { ...prev, selected_collections: selectedCollections } : prev);
      }

      if (loadingToast) {
        toast.dismiss(loadingToast);
      }
      
      // Close the modal if this was a newly created chat
      if (newlyCreatedChat && newlyCreatedChat.id === chatId) {
        setShowSelectTablesModal(false);
        setNewlyCreatedChat(null);
        await handleSelectConnection(chatId);
      }
      
      return;
    } catch (error: any) {
      console.error('Failed to update selected tables:', error);
      
      if (loadingToast) {
        toast.dismiss(loadingToast);
      }
      
      toast.error(error.message || 'Failed to update tables selection', {
        style: {
          background: '#000',
          color: '#fff',
          borderRadius: '12px',
          border: '4px solid #000',
        },
      });
      
      throw error; // Re-throw to allow the calling component to handle the error
    }
  };

  const handleUpdateAutoExecuteQuery = async (chatId: string, autoExecuteQuery: boolean): Promise<void> => {
    try {
      const updatedChat = await chatService.updateAutoExecuteQuery(chatId, autoExecuteQuery);
      
      // Update the chat in the state
      setChats(prev => prev.map(chat => 
        updatedChat.id === chatId ? { ...chat, auto_execute_query: autoExecuteQuery } : chat
      ));
      
      // If this is the selected connection, update it too
      if (selectedConnection?.id === chatId) {
        setSelectedConnection(prev => prev ? { ...prev, auto_execute_query: autoExecuteQuery } : prev);
      }
      
    } catch (error: any) {
      console.error('Failed to update auto execute query setting:', error);
      toast.error(error.message, errorToast);
    }
  };

  const handleEditConnectionFromChatWindow = () => {
    setIsEditingConnection(true);
    setShowConnectionModal(true);
  };

  const handleDuplicateConnection = useCallback(async (chatId: string) => {
    try {
      console.log('Duplicating chat with ID:', chatId);
      
      // First, fetch the duplicated chat
      const duplicatedChat = await chatService.getChat(chatId);
      console.log('Retrieved duplicated chat:', duplicatedChat);
      
      // Add the duplicated chat to the chats list if it's not already there
      setChats(prev => {
        // Check if chat already exists in the list
        if (prev.some(c => c.id === duplicatedChat.id)) {
          console.log('Chat already exists in list, not adding duplicate');
          return prev;
        }
        return [...prev, duplicatedChat];
      });
      
      // Select the duplicated chat
      setSelectedConnection(duplicatedChat);
      
      // Show success message
      setSuccessMessage('Chat duplicated successfully!');
      
      // Setup connection for the new chat
      try {
        await setupSSEConnection(chatId);
        
        // Update connection status
        handleConnectionStatusChange(chatId, true, 'duplicate-connection');
      } catch (connectionError) {
        console.error('Failed to establish connection to duplicated chat:', connectionError);
        toast('Chat duplicated, but connection could not be established automatically. Try selecting the chat again.', {
          ...toastStyle,
          style: {
            ...toastStyle.style,
            background: '#ffcc00',
            color: '#000',
            border: '4px solid #e6b800'
          },
          icon: '⚠️'
        });
      }
      
    } catch (error: any) {
      console.error('Failed to handle duplicated chat:', error);
      toast.error(`Failed to access duplicated chat: ${error.message}`, errorToast);
    }
  }, [handleConnectionStatusChange, setupSSEConnection, toastStyle, errorToast, setSuccessMessage]);

  // Navigation handlers
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <div className="flex flex-col min-h-screen bg-dark-bg-primary text-dark-text-primary">
      {/* Mobile header - only visible on small screens */}
      <div className="fixed top-0 left-0 right-0 h-16 bg-dark-bg-secondary border-b border-dark-border-primary md:hidden z-40 flex items-center justify-between px-4">
        <div className="flex items-center gap-2">
          <Boxes className="w-8 h-8 text-accent-blue" />
          <h1 className="text-2xl font-bold font-display text-dark-text-primary">DataBot</h1>
        </div>
      </div>

      {/* Floating Navigation Bar */}
      <Sidebar
        isExpanded={isSidebarExpanded}
        onToggleExpand={() => setIsSidebarExpanded(!isSidebarExpanded)}
        connections={chats}
        isLoadingConnections={isLoadingChats}
        onSelectConnection={handleSelectConnection}
        onAddConnection={() => {
          setIsEditingConnection(false);
          setShowConnectionModal(true);
        }}
        onLogout={handleLogout}
        selectedConnection={selectedConnection}
        onDeleteConnection={handleDeleteConnection}
        onEditConnection={handleEditConnectionFromChatWindow}
        onDuplicateConnection={handleDuplicateConnection}
        onConnectionStatusChange={handleConnectionStatusChange}
        eventSource={eventSource}
        onNavigationChange={(nav) => {
          if (nav === 'home') {
            setSelectedConnection(undefined);
          }
        }}
      />

      {/* Main Content Area - with padding for the floating navbar */}
      <div className="flex-1 w-full mt-24 px-4 sm:px-6 md:px-8 lg:px-10 max-w-screen-2xl mx-auto">
        <Routes>
          <Route path="/" element={
            <WelcomeSection 
              isSidebarExpanded={isSidebarExpanded} 
              setShowConnectionModal={setShowConnectionModal} 
              toastStyle={toastStyle} 
            />
          } />
          
          <Route path="/chat" element={
            selectedConnection ? (
              <ChatWindow
                chat={selectedConnection}
                isExpanded={isSidebarExpanded}
                messages={messages}
                checkSSEConnection={checkSSEConnection}
                setMessages={setMessages}
                onSendMessage={handleSendMessage}
                onClearChat={handleClearChat}
                onEditMessage={handleEditMessage}
                onCloseConnection={handleCloseConnection}
                onEditConnection={handleEditConnection}
                onConnectionStatusChange={handleConnectionStatusChange}
                isConnected={!!connectionStatuses[selectedConnection.id]}
                onCancelStream={handleCancelStream}
                onRefreshSchema={handleRefreshSchema}
                onCancelRefreshSchema={handleCancelRefreshSchema}
                onUpdateSelectedCollections={(chatId, selectedCollections) => handleUpdateSelectedCollections(chatId, selectedCollections)}
                onEditConnectionFromChatWindow={handleEditConnectionFromChatWindow}
              />
            ) : (
              <div className="flex flex-col items-center justify-center h-full py-16">
                <h2 className="text-2xl font-semibold mb-4">No Connection Selected</h2>
                <p className="text-dark-text-secondary mb-6">Please select a connection from the sidebar or create a new one.</p>
                <button
                  onClick={() => {
                    setIsEditingConnection(false);
                    setShowConnectionModal(true);
                  }}
                  className="px-4 py-2 bg-accent-blue hover:bg-accent-blue_dark text-white rounded-lg transition-colors"
                >
                  Create Connection
                </button>
              </div>
            )
          } />
          
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>

      {showConnectionModal && (
        <ConnectionModal
          onClose={() => {
            setShowConnectionModal(false);
            setIsEditingConnection(false);
          }}
          onSubmit={handleAddConnection}
          onUpdateSelectedCollections={handleUpdateSelectedCollections}
          initialData={isEditingConnection ? selectedConnection : undefined}
          onEdit={isEditingConnection ? async (connection, settings) => {
            try {
              const updatedChat = await chatService.editChat(selectedConnection!.id, connection, settings);
              
              // Update the chat in the state
              setChats(prev => prev.map(chat => 
                chat.id === updatedChat.id ? { ...updatedChat } : chat
              ));
              
              // Update the selected connection if it's the one being edited
              if (selectedConnection?.id === updatedChat.id) {
                setSelectedConnection(updatedChat);
              }
              
              toast.success('Connection updated successfully!', toastStyle);
              return { success: true };
            } catch (error: any) {
              console.error('Failed to update connection:', error);
              toast.error(error.message, errorToast);
              return { success: false, error: error.message };
            }
          } : undefined}
        />
      )}

      <Toaster
        position="bottom-center"
        reverseOrder={false}
        gutter={8}
        containerClassName="!fixed"
        containerStyle={{
          zIndex: 9999,
          bottom: '2rem'
        }}
        toastOptions={{
          success: {
            style: toastStyle.style,
            duration: 2000,
            icon: '👋',
          },
          error: {
            style: {
              ...toastStyle.style,
              background: '#ff4444',
              border: '4px solid #cc0000',
              color: '#fff',
              fontWeight: 'bold',
            },
            duration: 4000,
            icon: '⚠️',
          },
        }}
      />
      {successMessage && (
        <SuccessBanner
          message={successMessage}
          onClose={() => setSuccessMessage(null)}
        />
      )}
    </div>
  );
}

function App() {
  // Initialize analytics service
  useEffect(() => {
    try {
      // Initialize analytics with error handling
      const success = analyticsService.initAnalytics();
      if (success) {
        console.log('Analytics services initialized successfully');
      } else {
        console.log('Analytics initialization skipped - missing configuration');
      }
    } catch (error) {
      console.error('Failed to initialize analytics:', error);
    }
  }, []);
  
  return (
    <ThemeProvider>
      <UserProvider>
        <StreamProvider>
          <AppContent />
        </StreamProvider>
      </UserProvider>
    </ThemeProvider>
  );
}

export default App;