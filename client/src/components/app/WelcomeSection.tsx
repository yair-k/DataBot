import { MessageSquare, Database, LineChart, ArrowRight, Code, Sparkles, Monitor, Zap, Server } from 'lucide-react';
import { DefaultToastOptions } from 'react-hot-toast';
import toast from 'react-hot-toast';
import { useTheme } from '../../contexts/ThemeContext';
import { useEffect, useState } from 'react';

const WelcomeSection = ({ isSidebarExpanded, setShowConnectionModal, toastStyle }: { isSidebarExpanded: boolean, setShowConnectionModal: (show: boolean) => void, toastStyle: DefaultToastOptions }) => {
    const { isDarkTheme } = useTheme();
    const [isVisible, setIsVisible] = useState(false);
    const [activeTab, setActiveTab] = useState('features');
    const [hoveredCard, setHoveredCard] = useState<string | null>(null);
    
    useEffect(() => {
        // Animation effect when component mounts
        const timer = setTimeout(() => {
            setIsVisible(true);
        }, 100);
        
        return () => clearTimeout(timer);
    }, []);

    const handleCardHover = (cardId: string | null) => {
        setHoveredCard(cardId);
    };
    
    return (
        <div className="flex-1 flex flex-col items-center w-full">
            {/* Hero Section with Animation */}
            <div 
                className={`w-full py-16 relative overflow-hidden transition-all duration-700 ease-out
                    ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'}`}
            >
                {/* Animated Gradient Background */}
                <div className="absolute inset-0 overflow-hidden">
                    <div className="absolute top-0 left-1/4 w-96 h-96 rounded-full bg-accent-blue/10 blur-3xl animate-float"></div>
                    <div className="absolute bottom-0 right-1/4 w-96 h-96 rounded-full bg-accent-teal/10 blur-3xl animate-float-delayed"></div>
                </div>

                <div className="relative z-10 max-w-5xl mx-auto text-center px-4">
                    <div className="inline-block p-4 rounded-2xl bg-gradient-to-r from-accent-blue/10 to-accent-teal/10 mb-6 animate-pulse">
                        <Sparkles className="w-12 h-12 text-accent-blue" />
                    </div>
                    
                    <h1 className="text-4xl md:text-5xl lg:text-6xl font-display font-bold mb-6 bg-gradient-to-r from-accent-blue to-accent-teal bg-clip-text text-transparent">
                        Welcome to DataBot
                    </h1>
                    
                    <p className={`text-xl mb-8 max-w-3xl mx-auto ${isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}`}>
                        Open-source AI-powered engine for seamless database interactions.
                        <br />
                        From SQL to NoSQL, explore and analyze your data through natural conversations.
                    </p>
                    
                    <button
                        onClick={() => setShowConnectionModal(true)}
                        className="neo-button text-lg px-8 py-4 group transform transition-all duration-300 hover:scale-105"
                    >
                        <span className="flex items-center gap-2">
                            Create New Connection
                            <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                        </span>
                    </button>
                </div>
            </div>
            
            {/* Tab Navigation */}
            <div 
                className={`w-full max-w-5xl mx-auto mb-10 border-b border-light-border-primary dark:border-dark-border-primary px-4
                    transition-all duration-500 ease-out ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'}`}
                style={{ transitionDelay: '200ms' }}
            >
                <div className="flex space-x-8">
                    <button 
                        onClick={() => setActiveTab('features')}
                        className={`pb-4 px-2 relative ${activeTab === 'features' 
                            ? 'text-accent-blue' 
                            : isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}`}
                    >
                        Features
                        {activeTab === 'features' && (
                            <span className="absolute bottom-0 left-0 w-full h-1 bg-accent-blue rounded-t-lg"></span>
                        )}
                    </button>
                    
                    <button 
                        onClick={() => setActiveTab('databases')}
                        className={`pb-4 px-2 relative ${activeTab === 'databases' 
                            ? 'text-accent-blue' 
                            : isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}`}
                    >
                        Supported Databases
                        {activeTab === 'databases' && (
                            <span className="absolute bottom-0 left-0 w-full h-1 bg-accent-blue rounded-t-lg"></span>
                        )}
                    </button>
                    
                    <button 
                        onClick={() => setActiveTab('getting-started')}
                        className={`pb-4 px-2 relative ${activeTab === 'getting-started' 
                            ? 'text-accent-blue' 
                            : isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}`}
                    >
                        Getting Started
                        {activeTab === 'getting-started' && (
                            <span className="absolute bottom-0 left-0 w-full h-1 bg-accent-blue rounded-t-lg"></span>
                        )}
                    </button>
                </div>
            </div>
            
            {/* Features Tab */}
            {activeTab === 'features' && (
                <div 
                    className={`w-full max-w-5xl mx-auto grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-16 px-4
                        transition-all duration-500 ease-out ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'}`}
                    style={{ transitionDelay: '300ms' }}
                >
                    {/* Feature Card 1 */}
                    <div 
                        className={`
                            rounded-xl overflow-hidden transition-all duration-300
                            ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}
                            ${hoveredCard === 'nlq' ? 'transform scale-[1.02] shadow-lg' : ''}
                        `}
                        onMouseEnter={() => handleCardHover('nlq')}
                        onMouseLeave={() => handleCardHover(null)}
                    >
                        <div className="h-2 bg-gradient-to-r from-accent-blue to-accent-teal"></div>
                        <div className="p-6">
                            <div className="w-14 h-14 bg-accent-blue/10 rounded-2xl flex items-center justify-center mb-5">
                                <MessageSquare className="w-7 h-7 text-accent-blue" />
                            </div>
                            <h3 className="text-xl font-display font-medium mb-3">
                                Natural Language Queries
                            </h3>
                            <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                Talk to your database in plain English. DataBot translates your questions into database queries automatically.
                            </p>
                            
                            <button 
                                onClick={() => {
                                    toast.success('Talk to your database in plain English. DataBot translates your questions into database queries automatically.', toastStyle);
                                }}
                                className={`
                                    mt-5 px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors
                                    ${isDarkTheme ? 'text-accent-blue hover:bg-dark-bg-tertiary' : 'text-accent-blue hover:bg-light-bg-tertiary'}
                                `}
                            >
                                Learn more
                                <ArrowRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                    
                    {/* Feature Card 2 */}
                    <div 
                        className={`
                            rounded-xl overflow-hidden transition-all duration-300
                            ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}
                            ${hoveredCard === 'multidb' ? 'transform scale-[1.02] shadow-lg' : ''}
                        `}
                        onMouseEnter={() => handleCardHover('multidb')}
                        onMouseLeave={() => handleCardHover(null)}
                        onClick={() => setShowConnectionModal(true)}
                    >
                        <div className="h-2 bg-gradient-to-r from-accent-teal to-accent-blue"></div>
                        <div className="p-6">
                            <div className="w-14 h-14 bg-accent-blue/10 rounded-2xl flex items-center justify-center mb-5">
                                <Database className="w-7 h-7 text-accent-blue" />
                            </div>
                            <h3 className="text-xl font-display font-medium mb-3">
                                Multi-Database Support
                            </h3>
                            <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                Connect to PostgreSQL, MySQL, MongoDB, Redis, and more. One interface for all your databases.
                            </p>
                            
                            <button 
                                className={`
                                    mt-5 px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors
                                    ${isDarkTheme ? 'text-accent-blue hover:bg-dark-bg-tertiary' : 'text-accent-blue hover:bg-light-bg-tertiary'}
                                `}
                            >
                                Add connection
                                <ArrowRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                    
                    {/* Feature Card 3 */}
                    <div 
                        className={`
                            rounded-xl overflow-hidden transition-all duration-300
                            ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}
                            ${hoveredCard === 'viz' ? 'transform scale-[1.02] shadow-lg' : ''}
                        `}
                        onMouseEnter={() => handleCardHover('viz')}
                        onMouseLeave={() => handleCardHover(null)}
                    >
                        <div className="h-2 bg-gradient-to-r from-accent-blue to-accent-teal"></div>
                        <div className="p-6">
                            <div className="w-14 h-14 bg-accent-blue/10 rounded-2xl flex items-center justify-center mb-5">
                                <LineChart className="w-7 h-7 text-accent-blue" />
                            </div>
                            <h3 className="text-xl font-display font-medium mb-3">
                                Visualize Results
                            </h3>
                            <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                View your data in tables or JSON format. Execute queries and see results in real-time.
                            </p>
                            
                            <button 
                                onClick={() => {
                                    toast.success('Your data is visualized in tables or JSON format. Execute queries and see results in real-time.', toastStyle);
                                }}
                                className={`
                                    mt-5 px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors
                                    ${isDarkTheme ? 'text-accent-blue hover:bg-dark-bg-tertiary' : 'text-accent-blue hover:bg-light-bg-tertiary'}
                                `}
                            >
                                Learn more
                                <ArrowRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                    
                    {/* Feature Card 4 */}
                    <div 
                        className={`
                            rounded-xl overflow-hidden transition-all duration-300
                            ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}
                            ${hoveredCard === 'sql' ? 'transform scale-[1.02] shadow-lg' : ''}
                        `}
                        onMouseEnter={() => handleCardHover('sql')}
                        onMouseLeave={() => handleCardHover(null)}
                    >
                        <div className="h-2 bg-gradient-to-r from-accent-teal to-accent-blue"></div>
                        <div className="p-6">
                            <div className="w-14 h-14 bg-accent-blue/10 rounded-2xl flex items-center justify-center mb-5">
                                <Code className="w-7 h-7 text-accent-blue" />
                            </div>
                            <h3 className="text-xl font-display font-medium mb-3">
                                SQL Generation
                            </h3>
                            <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                DataBot analyzes your database structure and generates optimized queries automatically.
                            </p>
                            
                            <button 
                                onClick={() => {
                                    toast.success('DataBot automatically generates optimized SQL queries based on your database schema.', toastStyle);
                                }}
                                className={`
                                    mt-5 px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors
                                    ${isDarkTheme ? 'text-accent-blue hover:bg-dark-bg-tertiary' : 'text-accent-blue hover:bg-light-bg-tertiary'}
                                `}
                            >
                                Learn more
                                <ArrowRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                    
                    {/* Feature Card 5 */}
                    <div 
                        className={`
                            rounded-xl overflow-hidden transition-all duration-300
                            ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}
                            ${hoveredCard === 'realtime' ? 'transform scale-[1.02] shadow-lg' : ''}
                        `}
                        onMouseEnter={() => handleCardHover('realtime')}
                        onMouseLeave={() => handleCardHover(null)}
                    >
                        <div className="h-2 bg-gradient-to-r from-accent-blue to-accent-teal"></div>
                        <div className="p-6">
                            <div className="w-14 h-14 bg-accent-blue/10 rounded-2xl flex items-center justify-center mb-5">
                                <Zap className="w-7 h-7 text-accent-blue" />
                            </div>
                            <h3 className="text-xl font-display font-medium mb-3">
                                Real-time Analysis
                            </h3>
                            <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                Get instant insights from your data with lightning-fast query execution and streaming results.
                            </p>
                            
                            <button 
                                onClick={() => {
                                    toast.success('Get instant insights with real-time query execution and streaming results.', toastStyle);
                                }}
                                className={`
                                    mt-5 px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors
                                    ${isDarkTheme ? 'text-accent-blue hover:bg-dark-bg-tertiary' : 'text-accent-blue hover:bg-light-bg-tertiary'}
                                `}
                            >
                                Learn more
                                <ArrowRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                    
                    {/* Feature Card 6 */}
                    <div 
                        className={`
                            rounded-xl overflow-hidden transition-all duration-300
                            ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}
                            ${hoveredCard === 'schema' ? 'transform scale-[1.02] shadow-lg' : ''}
                        `}
                        onMouseEnter={() => handleCardHover('schema')}
                        onMouseLeave={() => handleCardHover(null)}
                    >
                        <div className="h-2 bg-gradient-to-r from-accent-teal to-accent-blue"></div>
                        <div className="p-6">
                            <div className="w-14 h-14 bg-accent-blue/10 rounded-2xl flex items-center justify-center mb-5">
                                <Server className="w-7 h-7 text-accent-blue" />
                            </div>
                            <h3 className="text-xl font-display font-medium mb-3">
                                Schema Detection
                            </h3>
                            <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                DataBot automatically analyzes your database structure to provide intelligent context-aware responses.
                            </p>
                            
                            <button 
                                onClick={() => {
                                    toast.success('DataBot automatically analyzes your database structure for intelligent responses.', toastStyle);
                                }}
                                className={`
                                    mt-5 px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors
                                    ${isDarkTheme ? 'text-accent-blue hover:bg-dark-bg-tertiary' : 'text-accent-blue hover:bg-light-bg-tertiary'}
                                `}
                            >
                                Learn more
                                <ArrowRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>
                </div>
            )}
            
            {/* Databases Tab */}
            {activeTab === 'databases' && (
                <div 
                    className={`w-full max-w-5xl mx-auto transition-all duration-500 ease-out px-4 mb-16
                        ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'}`}
                    style={{ transitionDelay: '300ms' }}
                >
                    <div className={`rounded-xl p-6 ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}`}>
                        <h3 className="text-2xl font-display font-medium mb-6">Supported Database Systems</h3>
                        
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                            {['PostgreSQL', 'MySQL', 'MongoDB', 'Redis', 'ClickHouse', 'Neo4j', 'YugabyteDB'].map((db, index) => (
                                <div key={index} className={`p-4 rounded-lg flex items-center gap-3 transition-colors
                                    ${isDarkTheme ? 'bg-dark-bg-tertiary' : 'bg-light-bg-tertiary'}`}>
                                    <div className="w-10 h-10 bg-accent-blue/10 rounded-lg flex items-center justify-center">
                                        <Database className="w-5 h-5 text-accent-blue" />
                                    </div>
                                    <span className="font-medium">{db}</span>
                                </div>
                            ))}
                        </div>
                        
                        <div className="mt-8 p-4 rounded-lg bg-accent-blue/5 flex items-center">
                            <div className="w-10 h-10 bg-accent-blue/10 rounded-lg flex items-center justify-center mr-4">
                                <Zap className="w-5 h-5 text-accent-blue" />
                            </div>
                            <p>
                                More database integrations coming soon. Have a specific database you'd like to see supported?
                                <a href="https://github.com/BeyondCodeBootcamp/DataBot" target="_blank" rel="noopener noreferrer" className="text-accent-blue ml-1">Let us know!</a>
                            </p>
                        </div>
                    </div>
                </div>
            )}
            
            {/* Getting Started Tab */}
            {activeTab === 'getting-started' && (
                <div 
                    className={`w-full max-w-5xl mx-auto transition-all duration-500 ease-out px-4 mb-16
                        ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'}`}
                    style={{ transitionDelay: '300ms' }}
                >
                    <div className={`rounded-xl p-6 ${isDarkTheme ? 'bg-dark-bg-secondary' : 'bg-light-bg-secondary'}`}>
                        <h3 className="text-2xl font-display font-medium mb-6">Getting Started with DataBot</h3>
                        
                        <div className="space-y-6">
                            <div className={`p-5 rounded-lg border-l-4 border-accent-blue
                                ${isDarkTheme ? 'bg-dark-bg-tertiary' : 'bg-light-bg-tertiary'}`}>
                                <h4 className="text-lg font-medium mb-2 flex items-center gap-2">
                                    <span className="w-6 h-6 rounded-full bg-accent-blue flex items-center justify-center text-white font-medium">1</span>
                                    Create a Connection
                                </h4>
                                <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                    Click the "Create New Connection" button and enter your database credentials.
                                    DataBot supports various database systems including PostgreSQL, MySQL, MongoDB, and more.
                                </p>
                            </div>
                            
                            <div className={`p-5 rounded-lg border-l-4 border-accent-blue
                                ${isDarkTheme ? 'bg-dark-bg-tertiary' : 'bg-light-bg-tertiary'}`}>
                                <h4 className="text-lg font-medium mb-2 flex items-center gap-2">
                                    <span className="w-6 h-6 rounded-full bg-accent-blue flex items-center justify-center text-white font-medium">2</span>
                                    Select Tables/Collections
                                </h4>
                                <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                    Choose which tables or collections you want DataBot to analyze.
                                    This helps DataBot understand your schema and optimize query performance.
                                </p>
                            </div>
                            
                            <div className={`p-5 rounded-lg border-l-4 border-accent-blue
                                ${isDarkTheme ? 'bg-dark-bg-tertiary' : 'bg-light-bg-tertiary'}`}>
                                <h4 className="text-lg font-medium mb-2 flex items-center gap-2">
                                    <span className="w-6 h-6 rounded-full bg-accent-blue flex items-center justify-center text-white font-medium">3</span>
                                    Start Chatting
                                </h4>
                                <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                    Begin asking questions about your data in plain English.
                                    DataBot will translate your queries, execute them, and show you the results instantly.
                                </p>
                            </div>
                        </div>
                        
                        <button
                            onClick={() => setShowConnectionModal(true)}
                            className="neo-button w-full mt-8 py-3 group"
                        >
                            <span className="flex items-center justify-center gap-2">
                                Start Now
                                <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                            </span>
                        </button>
                    </div>
                </div>
            )}
            
            {/* Bottom CTA Banner */}
            <div 
                className={`w-full max-w-5xl mx-auto px-4 mb-16
                    transition-all duration-500 ease-out ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'}`}
                style={{ transitionDelay: '400ms' }}
            >
                <div className={`
                    rounded-xl p-8 relative overflow-hidden
                    bg-gradient-to-r from-accent-blue/20 to-accent-teal/20
                `}>
                    <div className="absolute inset-0 overflow-hidden">
                        <div className="absolute -top-32 -right-32 w-64 h-64 rounded-full bg-accent-blue/10 blur-3xl"></div>
                        <div className="absolute -bottom-32 -left-32 w-64 h-64 rounded-full bg-accent-teal/10 blur-3xl"></div>
                    </div>
                    
                    <div className="relative z-10 flex flex-col md:flex-row items-center gap-6">
                        <div className="flex-1">
                            <h3 className="text-2xl font-display font-medium mb-2">Ready to start exploring your data?</h3>
                            <p className={isDarkTheme ? 'text-dark-text-secondary' : 'text-light-text-secondary'}>
                                Connect to your database and start asking questions in natural language.
                            </p>
                        </div>
                        
                        <button
                            onClick={() => setShowConnectionModal(true)}
                            className="neo-button py-3 px-6 whitespace-nowrap group"
                        >
                            <span className="flex items-center gap-2">
                                Connect Database
                                <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                            </span>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default WelcomeSection;