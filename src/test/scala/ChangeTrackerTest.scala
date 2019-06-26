import database.{ChangeTracker, MyConfig}
import junit.framework.TestCase

class ChangeTrackerTest extends TestCase {

  def testBatchComputationComplete(): Unit = {
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/batch-tracker-test.conf"))
    val changeTracker: ChangeTracker = pipeline_batch.start()
    print(changeTracker.pprintSimple())

    assert(changeTracker._schemaElementsAdded == 4)
    assert(changeTracker._schemaElementsDeleted == 0)


    assert(changeTracker._payloadElementsChanged == 2)
    assert(changeTracker._payloadEntriesAdded == 2)
    assert(changeTracker._payloadEntriesRemoved == 0)


    assert(changeTracker._instancesWithChangedSchema == 0)
    assert(changeTracker._instancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker._instancesNotChanged == 0)
    assert(changeTracker._instancesNew == 2)
    assert(changeTracker._instancesDeleted == 0)


    assert(changeTracker._addedInstanceToSchemaLinks == 2)
    assert(changeTracker._removedInstanceToSchemaLinks == 0)

  }

  def testBatchComputationMinimal(): Unit = {
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/batch-tracker-test-min.conf"))
    val changeTracker: ChangeTracker = pipeline_batch.start()
    print(changeTracker.pprintSimple())


    assert(changeTracker._schemaElementsAdded > 0)
    assert(changeTracker._schemaElementsDeleted == 0)


    assert(changeTracker._payloadElementsChanged > 0)
    assert(changeTracker._payloadEntriesAdded > 0)
    assert(changeTracker._payloadEntriesRemoved == 0)


    assert(changeTracker._instancesWithChangedSchema == 0)
    assert(changeTracker._instancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker._instancesNotChanged == 0)
    assert(changeTracker._instancesNew > 0)
    assert(changeTracker._instancesDeleted == 0)


    assert(changeTracker._addedInstanceToSchemaLinks > 0)
    assert(changeTracker._removedInstanceToSchemaLinks == 0)
  }


  def testIncrementalComplete(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/inc-tracker-test-1.conf"))
    val changeTracker: ChangeTracker = pipeline_inc.start()
    print(changeTracker.pprintSimple())

    assert(changeTracker._schemaElementsAdded == 0)
    assert(changeTracker._schemaElementsDeleted == 0)


    assert(changeTracker._payloadElementsChanged == 2)
    assert(changeTracker._payloadEntriesAdded == 2)
    assert(changeTracker._payloadEntriesRemoved == 0)


    assert(changeTracker._instancesWithChangedSchema == 0)
    assert(changeTracker._instancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker._instancesNotChanged == 2)
    assert(changeTracker._instancesNew == 1)
    assert(changeTracker._instancesDeleted == 0)


    assert(changeTracker._addedInstanceToSchemaLinks == 1)
    assert(changeTracker._removedInstanceToSchemaLinks == 0)
  }

  def testIncrementalComplete_secondRound(): Unit = {

    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/inc-tracker-test-2.conf"))
    val changeTracker: ChangeTracker = pipeline_inc.start()

    print(changeTracker.pprintSimple())

    assert(changeTracker._schemaElementsAdded == 0)
    assert(changeTracker._schemaElementsDeleted == 0)


    assert(changeTracker._payloadElementsChanged == 2)
    assert(changeTracker._payloadEntriesAdded == 0)
    assert(changeTracker._payloadEntriesRemoved == 2)


    assert(changeTracker._instancesWithChangedSchema == 0)
    assert(changeTracker._instancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker._instancesNotChanged == 2)
    assert(changeTracker._instancesNew == 0)
    assert(changeTracker._instancesDeleted == 1)


    assert(changeTracker._addedInstanceToSchemaLinks == 0)
    assert(changeTracker._removedInstanceToSchemaLinks == 1)
  }

  def testIncrementalComplete_thirdRound(): Unit = {
    val pipeline_incSecond: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/inc-tracker-test-3.conf"))
    val changeTracker: ChangeTracker = pipeline_incSecond.start()


    print(changeTracker.pprintSimple())

    assert(changeTracker._schemaElementsAdded == 3)
    assert(changeTracker._schemaElementsDeleted == 3)


    assert(changeTracker._payloadElementsChanged == 0)
    assert(changeTracker._payloadEntriesAdded == 0)
    assert(changeTracker._payloadEntriesRemoved == 0)


    assert(changeTracker._instancesWithChangedSchema == 2)
    assert(changeTracker._instancesChangedBecauseOfNeighbors == 1)
    assert(changeTracker._instancesNotChanged == 0)
    assert(changeTracker._instancesNew == 0)
    assert(changeTracker._instancesDeleted == 0)


    assert(changeTracker._addedInstanceToSchemaLinks == 2)
    assert(changeTracker._removedInstanceToSchemaLinks == 2)
  }
}
