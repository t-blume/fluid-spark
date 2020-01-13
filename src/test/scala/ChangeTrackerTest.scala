import database.{ChangeTracker, MyConfig, SecondaryIndexMem}
import junit.framework.TestCase

class ChangeTrackerTest extends TestCase {

  def testBatchComputationComplete(): Unit = {
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/batch-tracker-test.conf"))
    val changeTracker: ChangeTracker = pipeline_batch.start(false)
    print(changeTracker.pprintSimple())

    assert(changeTracker.getSchemaElementsAdded == 4)
    assert(changeTracker.getSchemaElementsDeleted == 0)


    assert(changeTracker.getPayloadElementsChanged == 2)
    assert(changeTracker.getPayloadEntriesAdded == 2)
    assert(changeTracker.getPayloadEntriesRemoved == 0)


    assert(changeTracker.getInstancesWithChangedSchema == 0)
    assert(changeTracker.getInstancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker.getInstancesNotChanged == 0)
    assert(changeTracker.getInstancesNew == 2)
    assert(changeTracker.getInstancesDeleted == 0)


    assert(changeTracker.getAddedInstanceToSchemaLinks == 2)
    assert(changeTracker.getRemovedInstanceToSchemaLinks == 0)

  }

  def testBatchComputationMinimal(): Unit = {
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/batch-tracker-test-min.conf"))
    val changeTracker: ChangeTracker = pipeline_batch.start(false)
    print(changeTracker.pprintSimple())


    assert(changeTracker.getSchemaElementsAdded > 0)
    assert(changeTracker.getSchemaElementsDeleted == 0)


    assert(changeTracker.getPayloadElementsChanged > 0)
    assert(changeTracker.getPayloadEntriesAdded > 0)
    assert(changeTracker.getPayloadEntriesRemoved == 0)


    assert(changeTracker.getInstancesWithChangedSchema == 0)
    assert(changeTracker.getInstancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker.getInstancesNotChanged == 0)
    assert(changeTracker.getInstancesNew > 0)
    assert(changeTracker.getInstancesDeleted == 0)


    assert(changeTracker.getAddedInstanceToSchemaLinks > 0)
    assert(changeTracker.getRemovedInstanceToSchemaLinks == 0)
  }


  def testIncrementalComplete(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/inc-tracker-test-1.conf"))
    val changeTracker: ChangeTracker = pipeline_inc.start(false)
    print(changeTracker.pprintSimple())

    assert(changeTracker.getSchemaElementsAdded == 0)
    assert(changeTracker.getSchemaElementsDeleted == 0)


    assert(changeTracker.getPayloadElementsChanged == 2)
    assert(changeTracker.getPayloadEntriesAdded == 2)
    assert(changeTracker.getPayloadEntriesRemoved == 0)


    assert(changeTracker.getInstancesWithChangedSchema == 0)
    assert(changeTracker.getInstancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker.getInstancesNotChanged == 2)
    assert(changeTracker.getInstancesNew == 1)
    assert(changeTracker.getInstancesDeleted == 0)


    assert(changeTracker.getAddedInstanceToSchemaLinks == 1)
    assert(changeTracker.getRemovedInstanceToSchemaLinks == 0)
  }

  def testIncrementalComplete_secondRound(): Unit = {

    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/inc-tracker-test-2.conf"))
    val changeTracker: ChangeTracker = pipeline_inc.start(false)


    print(changeTracker.pprintSimple())

    assert(changeTracker.getSchemaElementsAdded == 0)
    assert(changeTracker.getSchemaElementsDeleted == 0)


    assert(changeTracker.getPayloadElementsChanged == 2)
    assert(changeTracker.getPayloadEntriesAdded == 0)
    assert(changeTracker.getPayloadEntriesRemoved == 2)


    assert(changeTracker.getInstancesWithChangedSchema == 0)
    assert(changeTracker.getInstancesChangedBecauseOfNeighbors == 0)
    assert(changeTracker.getInstancesNotChanged == 2)
    assert(changeTracker.getInstancesNew == 0)
    assert(changeTracker.getInstancesDeleted == 1)


    assert(changeTracker.getAddedInstanceToSchemaLinks == 0)
    assert(changeTracker.getRemovedInstanceToSchemaLinks == 1)
  }

  def testIncrementalComplete_thirdRound(): Unit = {
    val pipeline_incSecond: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/inc-tracker-test-3.conf"))
    val changeTracker: ChangeTracker = pipeline_incSecond.start(false)


    print(changeTracker.pprintSimple())
    print(SecondaryIndexMem.getInstance().toString)

    assert(changeTracker.getNewSchemaStructureObserved == 2)
    assert(changeTracker.getSchemaStructureDeleted == 2)

    assert(changeTracker.getSchemaElementsAdded == 3)
    assert(changeTracker.getSchemaElementsDeleted == 3)


    assert(changeTracker.getPayloadElementsChanged == 0)
    assert(changeTracker.getPayloadEntriesAdded == 0)
    assert(changeTracker.getPayloadEntriesRemoved == 0)


    assert(changeTracker.getInstancesWithChangedSchema == 2)
    assert(changeTracker.getInstancesChangedBecauseOfNeighbors == 1)
    assert(changeTracker.getInstancesNotChanged == 0)
    assert(changeTracker.getInstancesNew == 0)
    assert(changeTracker.getInstancesDeleted == 0)


    assert(changeTracker.getAddedInstanceToSchemaLinks == 2)
    assert(changeTracker.getRemovedInstanceToSchemaLinks == 2)
  }
}
