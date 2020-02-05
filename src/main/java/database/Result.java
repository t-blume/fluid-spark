package database;

import java.io.Serializable;

public class Result<T> implements Serializable {

    private static final Result instance = new Result(true, true);

    public static Result getInstance() {
        return instance;
    }


    public static void syncStaticMerge(Result other) {
        System.out.println("Sync merge");
        synchronized (instance) {
            System.out.println("..merge..");
            getInstance().mergeAll(other);
        }
    }

    private final boolean enableTimeTracking;
    private final boolean enableChangeTracking;
    public T _result;
    public long _timeSpentReadingPrimaryIndex;
    public long _timeSpentWritingPrimaryIndex;
    public long _timeSpentDeletingPrimaryIndex;

    public long _timeSpentReadingSecondaryIndex;
    public long _timeSpentWritingSecondaryIndex;
    public long _timeSpentDeletingSecondaryIndex;

    public ChangeTracker _changeTracker;

    public Result(boolean enableTimeTracking, boolean enableChangeTracking) {
        this(null, enableTimeTracking, enableChangeTracking);
    }

    public Result(T result, boolean enableTimeTracking, boolean enableChangeTracking) {
        this.enableTimeTracking = enableTimeTracking;
        this.enableChangeTracking = enableChangeTracking;
        if (enableTimeTracking) {
            _timeSpentReadingPrimaryIndex = 0L;
            _timeSpentWritingPrimaryIndex = 0L;
            _timeSpentDeletingPrimaryIndex = 0L;
            _timeSpentReadingSecondaryIndex = 0L;
            _timeSpentWritingSecondaryIndex = 0L;
            _timeSpentDeletingSecondaryIndex = 0L;
        }
        if (enableChangeTracking)
            _changeTracker = new ChangeTracker();
        _result = result;
    }

    public Result<T> mergeAll(Result other) {
        if (enableTimeTracking)
            mergeTimes(other);
        if (enableChangeTracking)
            _changeTracker.merge(other._changeTracker);

        return this;
    }

    public void mergeTimes(Result other) {
        if (enableTimeTracking) {
            _timeSpentReadingPrimaryIndex += other._timeSpentReadingPrimaryIndex;
            _timeSpentWritingPrimaryIndex += other._timeSpentWritingPrimaryIndex;
            _timeSpentDeletingPrimaryIndex += other._timeSpentDeletingPrimaryIndex;
            _timeSpentReadingSecondaryIndex += other._timeSpentReadingSecondaryIndex;
            _timeSpentWritingSecondaryIndex += other._timeSpentWritingSecondaryIndex;
            _timeSpentDeletingSecondaryIndex += other._timeSpentDeletingSecondaryIndex;
        }
    }


    public void resetScores() {
        _timeSpentReadingPrimaryIndex = 0L;
        _timeSpentWritingPrimaryIndex = 0L;
        _timeSpentDeletingPrimaryIndex = 0L;
        _timeSpentReadingSecondaryIndex = 0L;
        _timeSpentWritingSecondaryIndex = 0L;
        _timeSpentDeletingSecondaryIndex = 0L;
        if (_changeTracker != null)
            _changeTracker.resetScores();
    }
}
