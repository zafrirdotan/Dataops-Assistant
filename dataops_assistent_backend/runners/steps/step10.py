from pipeline_builder.registry.pipeline_registry_service import pipelineRegistryService
import asyncio

def run():
    pipeline_id = "bank_transactions_to_sqlite_and_csv_20251231_1149_20251231_115003_4ba143c0"
    # Now, update the image_id
    updated = asyncio.get_event_loop().run_until_complete(
        pipelineRegistryService.update_pipeline(pipeline_id, {"image_id": "test_image_456"})
    )
    print(f"Updated pipeline: {updated.pipeline_id}, image_id: {updated.image_id}")

def main():
    run()

if __name__ == "__main__":
    main()
