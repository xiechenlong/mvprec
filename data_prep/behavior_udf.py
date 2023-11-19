from odps.udf import annotate

@annotate('*->string')
class BehaviorFilter(object):
    def evaluate(self, behavior_sequence, sample_timestamp, num_ids, fill_zero):
        if behavior_sequence is None:
            # If behavior sequence is None, decide whether to fill with zeros based on fill_zero
            return ','.join(['0'] * num_ids) if fill_zero else ''

        # Parse behavior sequence
        behavior_list = [
            (int(timestamp_str), id_str) 
            for id_str, timestamp_str in (behavior.split(':') for behavior in behavior_sequence.split(',')) 
            if int(timestamp_str) < sample_timestamp
        ]
        
        # Sort by timestamp from far to near, and get the ID of the last num_ids behaviors
        result_ids = [id_str for _, id_str in sorted(behavior_list, key=lambda x: x[0])[:num_ids]]
        
        # If not enough behaviors, decide whether to fill with zeros based on fill_zero
        if len(result_ids) < num_ids and fill_zero:
            result_ids += ['0'] * (num_ids - len(result_ids))
        
        # Join with commas
        return ','.join(result_ids)


@annotate('*->bigint')
class BehaviorLen(object):
    def evaluate(self, behavior_sequence, sample_timestamp, num_ids):
        # If the behavior_sequence is None, return 0 directly
        if behavior_sequence is None:
            return 0

        # Calculate the length of the behavior sequence where the timestamp is less than the sample timestamp
        # A generator expression and sum function are used here to count the number of behaviors that meet the condition
        behavior_list_len = sum(
            1
            for id_str, timestamp_str in (behavior.split(':') for behavior in behavior_sequence.split(',')) 
            if int(timestamp_str) < sample_timestamp
        )

        # Return the smaller value between the length of behavior sequence and num_ids
        # This ensures that the returned result doesn't exceed num_ids
        return min(behavior_list_len, num_ids)