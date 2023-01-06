import requests
import time

class CpuTable:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def _request(self, sql):
        response = requests.get(
            f'http://{self.host}:{self.port}/exec',
            params={'query': sql}).json()
        return response

    def drop(self):
        response = self._request('drop table cpu')
        if response.get('ddl') == 'OK':
            print(f'Dropped table cpu')
            return True
        elif response.get('error', '').startswith('table does not exist'):
            print(f'Table cpu does not exist')
            return False
        else:
            raise RuntimeError(f'Failed to drop table cpu: {response}')

    def create(self):
        symbol_cols = [
            'hostname', 'region', 'datacenter', 'rack', 'os', 'arch',
            'team', 'service', 'service_version', 'service_environment']
        double_cols = [
            'usage_user', 'usage_system', 'usage_idle', 'usage_nice',
            'usage_iowait', 'usage_irq', 'usage_softirq', 'usage_steal',
            'usage_guest', 'usage_guest_nice']
        sql = f'''
            create table cpu (
                {', '.join(f'{col} symbol' for col in symbol_cols)},
                {', '.join(f'{col} double' for col in double_cols)},
                timestamp timestamp)
                    timestamp(timestamp)
                    partition by day
            '''
        response = self._request(sql)
        if response.get('ddl') == 'OK':
            print(f'Created table cpu')
        else:
            raise RuntimeError(f'Failed to create table cpu: {response}')

    def get_row_count(self):
        response = self._request('select count(*) from cpu')
        return response['dataset'][0][0]

    def block_until_rowcount(self, target_count, timeout=30.0):
        t0 = time.monotonic()
        while True:
            row_count = self.get_row_count()
            if row_count == target_count:
                return
            elif row_count > target_count:
                raise RuntimeError(
                    f'Row count {row_count} exceeds target {target_count}')
            if time.monotonic() - t0 > timeout:
                raise RuntimeError(
                    f'Timed out waiting for row count to reach {target_count}')
            time.sleep(0.1)
