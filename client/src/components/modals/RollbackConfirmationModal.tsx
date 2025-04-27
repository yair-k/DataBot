import { History, Loader, X } from 'lucide-react';

interface RollbackConfirmationModalProps {
    onConfirm: () => void;
    onCancel: () => void;
    isLoading?: boolean;
}

export default function RollbackConfirmationModal({
    onConfirm,
    onCancel,
    isLoading = false,
}: RollbackConfirmationModalProps) {
    return (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center p-4 z-50">
            <div className="bg-white neo-border rounded-lg w-full max-w-md">
                <div className="flex justify-between items-center p-6 border-b-4 border-black">
                    <div className="flex items-center gap-3">
                        <History className="w-6 h-6 text-yellow-400" />
                        <h2 className="text-2xl font-bold">Rollback Changes</h2>
                    </div>
                    <button
                        onClick={onCancel}
                        className="hover:bg-neo-gray rounded-lg p-2 transition-colors"
                        disabled={isLoading}
                    >
                        <X className="w-6 h-6" />
                    </button>
                </div>

                <div className="p-6">
                    <p className="text-gray-600 mb-6">
                        Are you sure you want to rollback the changes made by this query? This will run a reverse query to revert the changes.
                    </p>

                    <div className="flex gap-4">
                        <button
                            onClick={onConfirm}
                            disabled={isLoading}
                            className="neo-border bg-yellow-400 text-black px-4 py-2 font-bold text-base transition-all hover:translate-y-[-2px] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:translate-y-[0px] active:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] flex-1 flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:translate-y-0 disabled:hover:shadow-none"
                        >
                            {isLoading ? (
                                <Loader className="w-4 h-4 animate-spin" />
                            ) : (
                                <History className="w-4 h-4" />
                            )}
                            <span>{isLoading ? "Rolling back..." : "Rollback"}</span>
                        </button>
                        <button
                            onClick={onCancel}
                            disabled={isLoading}
                            className="neo-button-secondary flex-1 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            Cancel
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
} 