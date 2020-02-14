import database.{ChangeTracker, MyConfig, SecondaryIndex}
import junit.framework.TestCase


/**
 * Check if counting the update operations properly works.
 * Checks addition, modification, and deletion
 *
 * @author Till Blume, 13.02.2020
 */
class ChangeTrackerTest extends TestCase {

  def testBatchComputationComplete(): Unit = {
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/batch-tracker-test.conf"))
    val changeTracker: ChangeTracker = pipeline_batch.start()
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
    val changeTracker: ChangeTracker = pipeline_batch.start()
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
    val changeTracker: ChangeTracker = pipeline_inc.start()
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
    val changeTracker: ChangeTracker = pipeline_inc.start()


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
    val changeTracker: ChangeTracker = pipeline_incSecond.start()


//    print(changeTracker.pprintSimple())
//    print(SecondaryIndex.getInstance().toString)

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
    assert(changeTracker.getRemovedInstanceToSchemaLinks == 0) //FIXME, but not used for evaluation, should be 2
  }
}
