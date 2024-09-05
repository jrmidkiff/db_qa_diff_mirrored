import sqlalchemy as sa, time

def print_stmt(stmt: sa.sql, rowcount = None): 
    '''Print out an SQLAlchemy statement'''
    print(stmt, '\n')
    if rowcount != None and rowcount >= 0: 
        print(f'Rows affected: {rowcount:,}\n')


def print_petl(nrows: int, table):
    '''Print a petl table if row count > 0'''
    if nrows != 0:
        looked_table = table.look()
        print(looked_table)
    else:
        print(f'\t--')


class SimpleTimer():
    '''A simple timer, with ability to measure a "lap"
    ```
    timer = SimpleTimer()
    for x in range(3): 
        timer.start_lap()
        # run some code
        timer.end_lap()
    timer.end()
    ```
    '''
    one_minute = 60
    one_hour = one_minute * 60
    one_day = one_hour * 24

    def __init__(self): 
        self._start = time.time()
        self._lap_start = None

    def _format_elapsed(self, elapsed):
        days = elapsed // self.one_day
        remainder = elapsed - (days * self.one_day)
        hours = remainder // self.one_hour
        remainder = remainder - (hours * self.one_hour)
        minutes = remainder // self.one_minute
        remainder = remainder - (minutes * self.one_minute)
        seconds = remainder % self.one_minute
        
        if days > 0: 
            return f'{days:.0f} day(s), {hours:.0f} hour(s), {minutes:.0f} minute(s), and {seconds:.0f} second(s)'
        elif hours > 0: 
            return f'{hours:.0f} hour(s), {minutes:.0f} minute(s), and {seconds:.0f} second(s)'
        elif minutes > 0: 
            return f'{minutes:.0f} minute(s) and {seconds:.0f} second(s)'
        else:
            return f'{seconds:.0f} second(s)'

    def start_lap(self): 
        '''Begin recording a new "lap"'''
        self._lap_start = time.time()

    def end_lap(self): 
        '''Calculate elapsed time of most recent lap'''
        assert self._lap_start != None, "SimpleTimer lap ended before being started"
        elapsed = time.time() - self._lap_start
        print(f'Lap elapsed time: {self._format_elapsed(elapsed)}')
    
    def end(self): 
        '''Calculate elapsed time of timer'''
        elapsed = time.time() - self._start
        print(f'Timer elapsed time: {self._format_elapsed(elapsed)}')
