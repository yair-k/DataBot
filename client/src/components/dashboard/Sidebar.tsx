import { EventSourcePolyfill } from 'event-source-polyfill';
import {
  ArrowRight,
  Boxes,
  Clock,
  Copy,
  Loader2,
  MoreVertical,
  PanelLeft,
  PanelLeftClose,
  Pencil,
  Plus,
  Trash2,
  ChevronDown,
  LogOut,
  User,
  MessageSquare
} from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { toast } from 'react-hot-toast';
import { useNavigate, useLocation } from 'react-router-dom';
import { useUser } from '../../contexts/UserContext';
import { useTheme } from '../../contexts/ThemeContext';
import chatService from '../../services/chatService';
import analyticsService from '../../services/analyticsService';
import { Chat } from '../../types/chat';
import DatabaseLogo from '../icons/DatabaseLogos';
import ConfirmationModal from '../modals/ConfirmationModal';
import DeleteConnectionModal from '../modals/DeleteConnectionModal';
import DuplicateChatModal from '../modals/DuplicateChatModal';
import { DemoModal } from '../modals/DemoModal';

export interface Connection {
  id: string;
  name: string;
  type: 'postgresql' | 'yugabytedb' | 'mysql' | 'clickhouse' | 'mongodb' | 'redis' | 'neo4j';
}

interface SidebarProps {
  isExpanded: boolean;
  onToggleExpand: () => void;
  connections: Chat[];
  onSelectConnection: (id: string) => void;
  onAddConnection: () => void;
  onLogout: () => void;
  onDeleteConnection?: (id: string) => void;
  onEditConnection?: () => void;
  onDuplicateConnection?: (id: string) => void;
  selectedConnection?: Chat;
  isLoadingConnections: boolean;
  onConnectionStatusChange?: (chatId: string, isConnected: boolean, from: string) => void;
  eventSource: EventSourcePolyfill | null;
  onNavigationChange?: (nav: string) => void;
}

// Date formatting and grouping functions
const formatDate = (dateString: string) => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  });
};

// Function to get a user-friendly relative time (e.g., "2 hours ago")
const getRelativeTime = (dateString: string): string => {
  const date = new Date(dateString);
  const now = new Date();
  const diffTime = now.getTime() - date.getTime();
  const diffMinutes = Math.floor(diffTime / (1000 * 60));
  
  if (diffMinutes < 1) {
    return 'Just now';
  } else if (diffMinutes < 60) {
    return `${diffMinutes} minute${diffMinutes > 1 ? 's' : ''} ago`;
  } else {
    const diffHours = Math.floor(diffMinutes / 60);
    if (diffHours < 24) {
      return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`;
    } else {
      const diffDays = Math.floor(diffHours / 24);
      if (diffDays === 1) {
        return 'Yesterday';
      } else {
        return formatDate(dateString);
      }
    }
  }
};

// Function to determine the date group for a connection
const getDateGroup = (dateString: string): string => {
  const date = new Date(dateString);
  const now = new Date();
  const diffTime = now.getTime() - date.getTime();
  const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
  
  if (diffDays === 0) {
    return 'Today';
  } else if (diffDays < 7) {
    return '7 Days';
  } else if (diffDays < 30) {
    return '30 Days';
  } else {
    return date.toLocaleDateString('en-US', { year: 'numeric', month: 'long' });
  }
};

// Group connections by date categories
const groupConnectionsByDate = (connections: Chat[]): Map<string, Chat[]> => {
  const groups = new Map<string, Chat[]>();
  
  // Sort connections by updated_at date (newest first)
  const sortedConnections = [...connections].sort(
    (a, b) => new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime()
  );
  
  // Group connections by date
  sortedConnections.forEach(connection => {
    const group = getDateGroup(connection.updated_at);
    if (!groups.has(group)) {
      groups.set(group, []);
    }
    groups.get(group)?.push(connection);
  });
  
  return groups;
};

// Sort date groups in chronological order
const getSortedGroups = (groups: Map<string, Chat[]>): Array<[string, Chat[]]> => {
  const specialOrder: { [key: string]: number } = {
    'Today': 0,
    '7 Days': 1,
    '30 Days': 2
  };
  
  return Array.from(groups.entries()).sort((a, b) => {
    // Handle special groups first
    if (a[0] in specialOrder && b[0] in specialOrder) {
      return specialOrder[a[0]] - specialOrder[b[0]];
    } else if (a[0] in specialOrder) {
      return -1;
    } else if (b[0] in specialOrder) {
      return 1;
    }
    
    // Sort other groups (months) by date, newest first
    const dateA = new Date(a[0]);
    const dateB = new Date(b[0]);
    return dateB.getTime() - dateA.getTime();
  });
};

export default function Sidebar({
  isExpanded,
  onToggleExpand,
  connections,
  onSelectConnection,
  onAddConnection,
  onLogout,
  onDeleteConnection,
  onEditConnection,
  onDuplicateConnection,
  selectedConnection,
  isLoadingConnections,
  onConnectionStatusChange,
  eventSource,
  onNavigationChange
}: SidebarProps) {
  const navigate = useNavigate();
  const location = useLocation();
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);
  const [connectionToDelete, setConnectionToDelete] = useState<Chat | null>(null);
  const [connectionToDuplicate, setConnectionToDuplicate] = useState<Chat | null>(null);
  const [currentConnectedChatId, setCurrentConnectedChatId] = useState<string | null>(null);
  const previousConnectionRef = useRef<string | null>(null);
  const { user } = useUser();
  const [openConnectionMenu, setOpenConnectionMenu] = useState<string | null>(null);
  const [menuPosition, setMenuPosition] = useState<{ top: number; left: number } | null>(null);
  const [showDemoModal, setShowDemoModal] = useState(false);
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    'Today': true,
    '7 Days': true,
    '30 Days': true
  });

  // Determine if we're on the home page or chat interface
  const isHomePage = location.pathname === '/';
  const isChatPage = location.pathname === '/chat' || location.pathname.startsWith('/chat/');

  // Toggle section expansion
  const toggleSection = (sectionName: string) => {
    setExpandedSections(prev => ({
      ...prev,
      [sectionName]: !prev[sectionName]
    }));
  };

  const handleNavigateToHome = () => {
    navigate('/');
    if (onNavigationChange) {
      onNavigationChange('home');
    }
  };

  const handleNavigateToChat = () => {
    navigate('/chat');
    if (onNavigationChange) {
      onNavigationChange('chat');
    }
  };
  
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (openConnectionMenu) {
        const target = event.target as HTMLElement;
        if (!target.closest('.connection-menu-container') && !target.closest('.connection-dropdown-menu')) {
          setOpenConnectionMenu(null);
          setMenuPosition(null);
        }
      }
    };
    
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [openConnectionMenu]);

  useEffect(() => {
    const selectedId = selectedConnection?.id;
    if (selectedId && selectedId !== previousConnectionRef.current) {
      handleSelectConnection(selectedId);
      previousConnectionRef.current = selectedId;
      
      // When a connection is selected, navigate to the chat interface
      if (!isChatPage) {
        navigate('/chat');
      }
    }
  }, [selectedConnection, isChatPage, navigate]);

  const handleToggleExpand = useCallback(() => {
    // Track sidebar toggled event
    analyticsService.trackSidebarToggled(!isExpanded);
    
    onToggleExpand();
  }, [isExpanded, onToggleExpand]);

  const handleLogoutClick = () => {
    setShowLogoutConfirm(true);
  };

  const handleLogoutConfirm = async () => {
    try {
      // If user exists, track logout event
      if (user) {
        analyticsService.trackLogout(user.id, user.username);
      }
      
      // Call the logout function passed from parent
      await onLogout();
      
      // Close the confirmation modal
      setShowLogoutConfirm(false);
    } catch (error) {
      console.error('Error during logout:', error);
      // Make sure to close the modal even if there's an error
      setShowLogoutConfirm(false);
    }
  };

  const handleEditConnection = (connection: Chat) => {
    setOpenConnectionMenu(null);
    
    // Track connection edit event
    analyticsService.trackConnectionEdited(
      connection.id,
      connection.connection.type,
      connection.connection.database
    );
    
    handleSelectConnection(connection.id);
    
    if (onEditConnection) {
      onEditConnection();
    }
  };

  const handleDeleteClick = useCallback((connection: Chat) => {
    setOpenConnectionMenu(null);
    setConnectionToDelete(connection);
  }, []);

  const handleDeleteConfirm = async (chatId: string) => {
    try {
      const connectionToDelete = connections.find(chat => chat.id === chatId);
      
      if (connectionToDelete) {
        // Track connection deleted event
        analyticsService.trackConnectionDeleted(
          chatId,
          connectionToDelete.connection.type,
          connectionToDelete.connection.database
        );
      }
      
      await chatService.deleteChat(chatId);
      if (onDeleteConnection) {
        onDeleteConnection(chatId);
      }
      setConnectionToDelete(null);
    } catch (error: any) {
      toast.error(error.message, {
        style: {
          background: '#ff4444',
          color: '#fff',
          border: '4px solid #cc0000',
          borderRadius: '12px',
          boxShadow: '4px 4px 0px 0px rgba(0,0,0,1)',
        },
      });
    }
  };

  const handleSelectConnection = useCallback(async (id: string) => {
    try {
      console.log('handleSelectConnection happened', { id, currentConnectedChatId });

      if (id === currentConnectedChatId) {
        onSelectConnection(id);
        return;
      }

      // Track connection selected event
      const connection = connections.find(chat => chat.id === id);
      if (connection) {
        analyticsService.trackConnectionSelected(
          id,
          connection.connection.type,
          connection.connection.database
        );
      }
      
      setCurrentConnectedChatId(id);
      onSelectConnection(id);
      onConnectionStatusChange?.(id, false, 'sidebar-connecting');

      // Navigate to the chat interface when a connection is selected
      if (!isChatPage) {
        navigate('/chat');
      }

    } catch (error) {
      console.error('Failed to setup connection:', error);
      onConnectionStatusChange?.(id, false, 'sidebar-select-connection');
      toast.error('Failed to connect to database');
    }
  }, [currentConnectedChatId, onSelectConnection, onConnectionStatusChange, connections, navigate, isChatPage]);

  const handleOpenMenu = (e: React.MouseEvent, connectionId: string) => {
    e.preventDefault();
    e.stopPropagation();
    
    // Get the position of the button
    const button = e.currentTarget;
    const rect = button.getBoundingClientRect();
    
    // Set the position for the dropdown
    setMenuPosition({
      top: rect.top,
      left: rect.right + 10 // Position to the right of the button
    });
    
    // Toggle the menu
    setOpenConnectionMenu(openConnectionMenu === connectionId ? null : connectionId);
  };

  const handleDuplicateConnection = (connection: Chat) => {
    setOpenConnectionMenu(null);
    setConnectionToDuplicate(connection);
  };

  const handleDuplicateConfirm = async (chatId: string, duplicateMessages: boolean) => {
    try {
      const connectionToDuplicate = connections.find(chat => chat.id === chatId);
      
      if (connectionToDuplicate) {
        // Track connection duplicate event in analytics
        analyticsService.trackConnectionDuplicated(
          chatId,
          connectionToDuplicate.connection.type,
          connectionToDuplicate.connection.database,
          duplicateMessages
        );
      }

      // Call the duplicateChat service and get the duplicated chat
      const duplicatedChat = await chatService.duplicateChat(chatId, duplicateMessages);
      
      // Call the onDuplicateConnection callback with the ID of the duplicated chat
      if (onDuplicateConnection) {
        onDuplicateConnection(duplicatedChat.id);
      }
      
      toast.success('Chat duplicated successfully!', {
        style: {
          background: '#000',
          color: '#fff',
          border: '4px solid #000',
          borderRadius: '12px',
          boxShadow: '4px 4px 0px 0px rgba(0,0,0,1)',
        },
      });
      
      setConnectionToDuplicate(null);
    } catch (error: any) {
      toast.error(error.message, {
        style: {
          background: '#ff4444',
          color: '#fff',
          border: '4px solid #cc0000',
          borderRadius: '12px',
          boxShadow: '4px 4px 0px 0px rgba(0,0,0,1)',
        },
      });
    }
  };

  // Group connections by date
  const groupedConnections = groupConnectionsByDate(connections);
  const sortedGroups = getSortedGroups(groupedConnections);

  return (
    <>
      {/* Top Navigation Bar (always visible) */}
      <div
        className={`${isExpanded ? 'w-auto max-w-4xl' : 'w-auto'} 
          fixed top-4 left-1/2 -translate-x-1/2 
          bg-dark-bg-primary/90 backdrop-blur-lg border border-dark-border-primary
          rounded-full shadow-lg shadow-accent-blue/10
          flex flex-row transition-all duration-300 ease-in-out
          z-50 overflow-visible
          ${isExpanded ? 'h-auto py-3 px-5' : 'h-14 py-2 px-4'}`}
      >
        {/* Logo and App Name - Clickable to go home */}
        <div 
          className="flex items-center gap-3 pr-4 border-r border-dark-border-primary cursor-pointer hover:opacity-80 transition-opacity"
          onClick={handleNavigateToHome}
          title="Return to Home"
        >
          <div className="relative">
            <Boxes className="w-7 h-7 text-accent-blue" />
          </div>
          <h1 className={`text-xl font-display font-semibold transition-opacity duration-300 ${isExpanded ? 'opacity-100' : 'opacity-0 w-0'}`}>
            DataBot
          </h1>
        </div>

        {/* Main Action Button - Always Opens Chat Interface */}
        <div className="flex items-center px-4 gap-4">
          <button
            onClick={handleNavigateToChat}
            className={`neo-button-primary flex items-center gap-2 py-2 px-4 ${isChatPage ? 'bg-accent-blue' : 'bg-accent-blue hover:bg-accent-blue_dark'} text-white rounded-lg transition-colors`}
            title="Open Chat Interface"
          >
            <MessageSquare className="w-5 h-5" />
            {!isChatPage || isExpanded ? <span>Chat with Data</span> : null}
          </button>
          
          {/* New Connection Button - Only visible on chat page */}
          {isChatPage && (
            <button
              onClick={onAddConnection}
              className="neo-button-secondary flex items-center gap-2 py-2 px-3 rounded-lg text-dark-text-primary hover:bg-dark-bg-tertiary transition-colors"
              title="Add Connection"
            >
              <Plus className="w-5 h-5" />
              {isExpanded && <span>New Connection</span>}
            </button>
          )}
        </div>

        {/* Action Buttons */}
        <div className="flex items-center gap-2 pl-4 border-l border-dark-border-primary">
          {/* User Profile Dropdown */}
          <div className="relative">
            <button
              onClick={() => setOpenConnectionMenu(openConnectionMenu === 'user' ? null : 'user')}
              className="flex items-center gap-2 py-2 px-3 rounded-full hover:bg-dark-bg-tertiary transition-colors"
              title="User menu"
            >
              <div className="p-1 rounded-full bg-accent-blue/10">
                <User className="w-5 h-5 text-accent-blue" />
              </div>
              {isExpanded && (
                <>
                  <span className="text-dark-text-primary text-sm font-medium">
                    {user?.username || 'User'}
                  </span>
                  <ChevronDown className="w-4 h-4 text-dark-text-secondary" />
                </>
              )}
            </button>
            
            {/* User Dropdown Menu - Fixed position outside navbar */}
            {openConnectionMenu === 'user' && (
              <div 
                className="absolute right-0 mt-2 w-60 bg-dark-bg-secondary border border-dark-border-primary rounded-xl shadow-lg z-[999]"
                style={{ top: '100%' }}
              >
                <div className="p-4 border-b border-dark-border-primary">
                  <p className="font-medium text-dark-text-primary">
                    {user?.username || 'User'}
                  </p>
                  <p className="text-xs text-dark-text-tertiary">
                    Joined {formatDate(user?.created_at || '')}
                  </p>
                </div>
                <div className="p-2">
                  <button
                    onClick={() => {
                      handleLogoutClick();
                      setOpenConnectionMenu(null);
                    }}
                    className="w-full text-left px-3 py-2 rounded-lg text-neo-error hover:bg-neo-error/10 transition-colors flex items-center gap-2"
                  >
                    <LogOut className="w-4 h-4" />
                    <span>Logout</span>
                  </button>
                </div>
              </div>
            )}
          </div>
          
          {/* Toggle Sidebar Button - Only in chat interface */}
          {isChatPage && (
            <button
              onClick={handleToggleExpand}
              className="p-2 rounded-full bg-accent-blue hover:bg-accent-blue_dark text-white transition-colors"
              title={isExpanded ? "Collapse sidebar" : "Expand sidebar"}
            >
              {isExpanded ? (
                <PanelLeftClose className="w-5 h-5" />
              ) : (
                <PanelLeft className="w-5 h-5" />
              )}
            </button>
          )}
        </div>
      </div>

      {/* CHAT INTERFACE - Connections sidebar and chat area */}
      {isChatPage && (
        <>
          {/* Connection List Sidebar */}
          <div className="fixed left-4 top-24 bottom-6 w-72 bg-dark-bg-secondary border border-dark-border-primary rounded-xl shadow-lg overflow-y-auto scrollbar-theme z-30">
            <div className="p-4">
              <h2 className="text-lg font-display font-medium text-dark-text-primary mb-4">Your Connections</h2>
              
              {/* Connection list grouped by date */}
              {isLoadingConnections ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="w-6 h-6 text-accent-blue animate-spin" />
                </div>
              ) : connections.length === 0 ? (
                <div className="text-center py-8">
                  <p className="text-dark-text-tertiary">No connections found</p>
                  <button 
                    onClick={onAddConnection}
                    className="mt-4 px-4 py-2 bg-accent-blue hover:bg-accent-blue_dark text-white rounded-lg transition-colors"
                  >
                    Create a connection
                  </button>
                </div>
              ) : (
                <div className="space-y-4">
                  {sortedGroups.map(([group, chats]) => (
                    <div key={group} className="space-y-2">
                      <div 
                        className="flex items-center gap-2 cursor-pointer"
                        onClick={() => toggleSection(group)}
                      >
                        <ChevronDown className={`w-4 h-4 text-dark-text-tertiary transition-transform ${expandedSections[group] ? 'rotate-0' : '-rotate-90'}`} />
                        <h3 className="text-sm font-medium text-dark-text-secondary flex items-center gap-2">
                          <Clock className="w-3.5 h-3.5" />
                          {group}
                        </h3>
                      </div>
                      
                      {expandedSections[group] && (
                        <div className="space-y-1 pl-6">
                          {chats.map(chat => (
                            <div 
                              key={chat.id}
                              className={`flex items-center justify-between rounded-lg px-3 py-2 cursor-pointer group ${
                                selectedConnection?.id === chat.id 
                                  ? 'bg-accent-blue/10 text-accent-blue' 
                                  : 'hover:bg-dark-bg-tertiary text-dark-text-primary'
                              }`}
                              onClick={() => handleSelectConnection(chat.id)}
                            >
                              <div className="flex items-center gap-2 overflow-hidden">
                                <DatabaseLogo 
                                  type={chat.connection.type as any} 
                                  size={18} 
                                  className={selectedConnection?.id === chat.id ? 'text-accent-blue' : 'text-dark-text-secondary'} 
                                />
                                <div className="overflow-hidden">
                                  <p className="text-sm font-medium truncate">{chat.connection.is_example_db ? "Sample Database" : chat.connection.database}</p>
                                  <p className="text-xs text-dark-text-tertiary truncate">{chat.connection.host}</p>
                                </div>
                              </div>
                              
                              <button
                                onClick={(e) => handleOpenMenu(e, chat.id)}
                                className="p-1.5 rounded-lg opacity-0 group-hover:opacity-100 hover:bg-dark-bg-tertiary transition-colors connection-menu-container"
                              >
                                <MoreVertical className="w-4 h-4 text-dark-text-tertiary" />
                              </button>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </>
      )}

      {/* Modals - styling updated for dark theme */}
      {showLogoutConfirm && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center bg-dark-bg-primary/80 backdrop-blur-sm">
          <ConfirmationModal
            title="Confirm Logout"
            message="Are you sure you want to logout? Any unsaved changes will be lost."
            onConfirm={handleLogoutConfirm}
            onCancel={() => setShowLogoutConfirm(false)}
          />
        </div>
      )}

      {connectionToDelete && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center bg-dark-bg-primary/80 backdrop-blur-sm">
          <DeleteConnectionModal
            connectionName={connectionToDelete.connection.is_example_db ? "Sample Database" : connectionToDelete.connection.database}
            chatId={connectionToDelete.id}
            onConfirm={handleDeleteConfirm}
            onCancel={() => setConnectionToDelete(null)}
          />
        </div>
      )}

      {connectionToDuplicate && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center bg-dark-bg-primary/80 backdrop-blur-sm">
          <DuplicateChatModal
            chatName={connectionToDuplicate.connection.database}
            chatId={connectionToDuplicate.id}
            onConfirm={handleDuplicateConfirm}
            onCancel={() => setConnectionToDuplicate(null)}
          />
        </div>
      )}

      {/* Connection Dropdown Menu */}
      {openConnectionMenu && menuPosition && openConnectionMenu !== 'selector' && openConnectionMenu !== 'user' && (
        <div 
          className="fixed w-48 bg-dark-bg-secondary border border-dark-border-primary rounded-lg shadow-neo-dark z-[100] connection-dropdown-menu overflow-hidden"
          style={{
            top: `${menuPosition.top}px`,
            left: `${menuPosition.left}px`,
            transform: 'none'
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <div className="py-1">
            <button 
              onClick={() => {
                const connection = connections.find(c => c.id === openConnectionMenu);
                if (connection) {
                  handleEditConnection(connection);
                }
                setOpenConnectionMenu(null);
                setMenuPosition(null);
              }}
              className="flex items-center w-full text-left px-3 py-2 text-sm text-dark-text-primary hover:bg-dark-bg-tertiary transition-colors"
            >
              <Pencil className="w-4 h-4 mr-2 text-accent-blue" />
              Edit Connection
            </button>
            <div className="h-px bg-dark-border-primary mx-2"></div>
            <button 
              onClick={() => {
                const connection = connections.find(c => c.id === openConnectionMenu);
                if (connection) {
                  handleDuplicateConnection(connection);
                }
                setOpenConnectionMenu(null);
                setMenuPosition(null);
              }}
              className="flex items-center w-full text-left px-3 py-2 text-sm text-dark-text-primary hover:bg-dark-bg-tertiary transition-colors"
            >
              <Copy className="w-4 h-4 mr-2 text-accent-blue" />
              Duplicate Chat
            </button>
            <div className="h-px bg-dark-border-primary mx-2"></div>
            <button 
              onClick={() => {
                const connection = connections.find(c => c.id === openConnectionMenu);
                if (connection) {
                  handleDeleteClick(connection);
                }
                setOpenConnectionMenu(null);
                setMenuPosition(null);
              }}
              className="flex items-center w-full text-left px-3 py-2 text-sm text-neo-error hover:bg-neo-error/10 transition-colors"
            >
              <Trash2 className="w-4 h-4 mr-2 text-neo-error" />
              Delete Connection
            </button>
          </div>
        </div>
      )}

      {/* Demo Modal */}
      <DemoModal isOpen={showDemoModal} onClose={() => setShowDemoModal(false)} />
    </>
  );
}