"""

    This is a simple stand-alone script 
    (it requires only Bauplan installed and the relevant credentials)
    that showcase the WAP pattern in uploading parquet files to a lakehouse,
    i.e. Iceberg table backed by a catalog. In particular,
    the script will:
    
    * Ingest data from an S3 source into an Iceberg table
    * Run quality checks on the data using Bauplan and Arrow
    * Merge the branch into the main branch
    
    If dependencies are installed, just fix the global vars at the top of the script and run: 
    
    python wap_flow.py 
    
    Note how much lighter the integration is compared to other datalake tools ;-)

"""


### IMPORTS
from datetime import datetime
import bauplan
from prefect import flow, task
from prefect.transactions import transaction, get_transaction


@task
def source_to_iceberg_table(
    bauplan_client: bauplan.Client,
    table_name: str,
    source_s3_pattern: str,
    bauplan_ingestion_branch: str
):
    """
    
    Wrap the plan-upload and upload process in Bauplan so 
    that conflict resolution and upload can happen programmatically.
    
    """
    get_transaction().set("bauplan_ingestion_branch", bauplan_ingestion_branch)
    plan_state = bauplan_client.plan_import(
        search_string=source_s3_pattern,
        table_name=table_name
    )
    if plan_state.error:
        raise ValueError(f"Error planning the ingestion:  {plan_state.error}")
    # we assume the schema does not have conflicts and just apply it
    apply_state = bauplan_client.apply_import(
        plan=plan_state.plan,
        onto_branch=bauplan_ingestion_branch
    )
    if apply_state.error:
        raise ValueError(f"Error applying the resolved schema:  {apply_state.error}")

    return


@task
def run_quality_checks(
    bauplan_client: bauplan.Client,
    bauplan_ingestion_branch: str,
    table_name: str
):
    """
    
    We check the data quality by running the checks in-process: we use 
    Bauplan SDK to query the data as an Arrow table, and check if the 
    target column is not null through vectorized PyArrow operations.
    
    """
    get_transaction().set("bauplan_ingestion_branch", bauplan_ingestion_branch)
    # we retrieve the data and check if the table is column has any nulls
    column_to_check = 'pickup_datetime'
    # NOTE if you don't want to use any SQL, you can interact with the lakehouse in pure Python
    # and still back an Arrow table (in this one column) through a performant scan.
    print("Perform a S3 columnar scan on the column {}".format(column_to_check))
    wap_table = bauplan_client.scan(
        table_name=table_name,
        branch_name=bauplan_ingestion_branch,
        columns=[column_to_check]
    )
    print("Read the table successfully!")
    assert wap_table[column_to_check].null_count == 0, "Quality check failed"
    print("Quality check passed")
    
    return True


@task
def merge_branch(
    bauplan_client: bauplan.Client,
    bauplan_ingestion_branch: str
):
    """
    
    We merge the ingestion branch into the main branch. If this succeed,
    the transaction itself is considered successful.
    
    """
    get_transaction().set("bauplan_ingestion_branch", bauplan_ingestion_branch)
    # we merge the branch into the main branch
    return bauplan_client.merge_branch(
        onto_branch='main',
        from_ref=bauplan_ingestion_branch
    )


@source_to_iceberg_table.on_rollback
@run_quality_checks.on_rollback
@merge_branch.on_commit
def delete_branch_if_exists(transaction):
    """
    
    If the task fails or the merge succeeded, we delete the branch to avoid clutter!
    
    """
    _client = bauplan.Client()
    ingestion_branch = transaction.get('bauplan_ingestion_branch')
    if  ingestion_branch in [_.name for _ in _client.get_branches()]:
        print(f"Deleting the branch {ingestion_branch}")
        _client.delete_branch(ingestion_branch)
    else:
        print(f"Branch {ingestion_branch} does not exist, nothing to delete.")
        
    return


@flow(log_prints=True)
def wap_with_bauplan(
    bauplan_ingestion_branch: str, 
    source_s3_pattern: str,
    table_name: str
):
    """
    Run the WAP ingestion pipeline using Bauplan in a Prefect flow
    leveraging the new concept of transactions:
    
    https://docs-3.prefect.io/3.0rc/develop/transactions#write-your-first-transaction
    """
    print("Starting WAP at {}!".format(datetime.now()))
    bauplan_client = bauplan.Client()
    # start a Prefect transaction
    with transaction():
            ### THIS IS THE WRITE
            # first, ingest data from the s3 source into a table the Bauplan branch
            source_to_iceberg_table(
                bauplan_client,
                table_name, 
                source_s3_pattern,
                bauplan_ingestion_branch
            )
            ### THIS IS THE AUDIT
            # we query the table in the branch and check we have no nulls
            run_quality_checks(
                bauplan_client,
                bauplan_ingestion_branch, 
                table_name=table_name
            )
            # THIS IS THE PUBLISH 
            # finally, we merge the branch into the main branch if the quality checks passed
            merge_branch(
                bauplan_client,
                bauplan_ingestion_branch
            )
            
    # say goodbye
    print("All done at {}, see you, space cowboy.".format(datetime.now()))

    return


if __name__ == "__main__":
    # parse the args when the script is run from the command line
    import argparse
    parser = argparse.ArgumentParser()
    # table_name, branch_name and s3_path are the main arguments from
    # the command line
    parser.add_argument('--table_name', type=str)
    parser.add_argument('--branch_name', type=str)
    parser.add_argument('--s3_path', type=str)
    args = parser.parse_args()
    
    # the name of the table we will be ingesting data into
    table_name = args.table_name
    # the name of the data branch in which we will be ingesting data
    # NOTE: the name should start with your username as a prefix
    branch_name = args.branch_name
    # s3 pattern to the data we want to ingest
    # NOTE: if you're using Bauplan Alpha environmen 
    # this should be a publicly accessible path (list and get should be allowed)
    s3_path = args.s3_path
    print(f"Starting the WAP flow with the following parameters: {table_name}, {branch_name}, {s3_path}")
    # start the flow
    wap_with_bauplan(
        bauplan_ingestion_branch=branch_name,
        source_s3_pattern=s3_path,
        table_name=table_name
    )
    

