import React from 'react';

interface SettingsTabProps {
  autoExecuteQuery: boolean;
  shareWithAI: boolean;
  setAutoExecuteQuery: (value: boolean) => void;
  setShareWithAI: (value: boolean) => void;
}

const SettingsTab: React.FC<SettingsTabProps> = ({
  autoExecuteQuery,
  shareWithAI,
  setAutoExecuteQuery,
  setShareWithAI,
}) => {
  return (
    <div className="space-y-6">
      <div className="neo-border p-4 rounded-lg bg-light-bg-secondary dark:bg-dark-bg-secondary border-light-border-primary dark:border-dark-border-primary">
        <div className="flex flex-col md:flex-row gap-4 md:gap-0 items-start md:items-center justify-between">
          <div>
            <label className="block font-bold mb-1 text-lg text-light-text-primary dark:text-dark-text-primary">Auto Fetch Results</label>
            <p className="text-light-text-tertiary dark:text-dark-text-tertiary text-sm max-w-[480px]">
              Automatically fetches results of the AI operation from the database upon the AI response. <br />
              However, the critical operations such as Updating, Inserting, Deleting, etc. still need to be executed manually by the user.
            </p>
          </div>
          <label className="relative inline-flex items-center cursor-pointer">
            <input 
              type="checkbox" 
              className="sr-only peer" 
              checked={autoExecuteQuery}
              onChange={(e) => {
                const newValue = e.target.checked;
                setAutoExecuteQuery(newValue);
              }}
            />
            <div className="w-11 h-6 bg-light-border-primary dark:bg-dark-border-primary peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-accent-blue/30 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-light-border-primary dark:after:border-dark-border-secondary after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-accent-blue"></div>
          </label>
        </div>
      </div>

      <div className="neo-border p-4 rounded-lg bg-light-bg-secondary dark:bg-dark-bg-secondary border-light-border-primary dark:border-dark-border-primary">
        <div className="flex flex-col md:flex-row gap-4 md:gap-0 items-start md:items-center justify-between">
          <div>
            <label className="block font-bold mb-1 text-lg text-light-text-primary dark:text-dark-text-primary">Share Data With AI</label>
            <p className="text-light-text-tertiary dark:text-dark-text-tertiary text-sm max-w-[480px]">
              Allow DataBot to share your operation results with AI for better responses. This can be useful for analysing data such as comparsions. <br/>
              Note: This will take more tokens that are being sent to the AI.
            </p>
          </div>
          <label className="relative inline-flex items-center cursor-pointer">
            <input 
              type="checkbox" 
              className="sr-only peer" 
              checked={shareWithAI}
              onChange={(e) => {
                setShareWithAI(e.target.checked);
              }}
            />
            <div className="w-11 h-6 bg-light-border-primary dark:bg-dark-border-primary peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-accent-blue/30 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-light-border-primary dark:after:border-dark-border-secondary after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-accent-blue"></div>
          </label>
        </div>
      </div>
    </div>
  );
};

export default SettingsTab;