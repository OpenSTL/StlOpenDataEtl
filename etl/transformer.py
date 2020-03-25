from .transformer_tasks.vacant_table.vacant_table import vacant_table

class Transformer:
    def __init__(self):
        self.transform_tasks_to_transform_fns = {
            'vacant_table': vacant_table
        }

    def transform_all(self, extracted, transform_task_list):
        '''
        Runs transform tasks on the extracted data.
        Each transform tasks ingests the extracted data and outputs
        one or more transformed dataframes.
        A transform task is a function that receives one parameter:
        a dictionary of dataframes from the extractor

        Arguments:
        extracted -- Extractor output
        transform_task_list -- List of transform tasks to run

        Returns:
        A dictionary of the output dataframes like:
        transformed['task_name'] = <output from transformer task>

        '''
        transformed = {}

        for task_name in transform_task_list:
            transform_task_fn = self.transform_tasks_to_transform_fns.get(
                task_name,
                'not_found'
            )
            if (transform_task_fn == 'not_found'):
                print('unknown transform task ' + task_name)
                continue
            transformed[task_name] = transform_task_fn(extracted)
        
        return transformed
        