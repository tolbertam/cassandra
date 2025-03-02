# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timezone

from cassandra.query import QueryTrace, TraceUnavailable
from cqlshlib.displaying import MAGENTA


def print_trace_session(shell, session, session_id, partial_session=False):
    """
    Lookup a trace by session and trace session ID, then print it.
    """
    trace = QueryTrace(session_id, session)
    try:
        wait_for_complete = not partial_session
        trace.populate(wait_for_complete=wait_for_complete)
    except TraceUnavailable:
        shell.printerr("Session %s wasn't found." % session_id)
    else:
        print_trace(shell, trace)


def print_trace(shell, trace):
    """
    Print an already populated cassandra.query.QueryTrace instance.
    """
    temp_query = None
    temp_color = None
    if shell.shunted_query_out is not None:
        temp_query = shell.query_out
        shell.query_out = shell.shunted_query_out
        temp_color = shell.color
        shell.color = shell.shunted_color

    rows = make_trace_rows(trace)
    if not rows:
        shell.printerr("No rows for session %s found." % (trace.trace_id,))
        return
    names = ['activity', 'timestamp', 'source', 'source_elapsed', 'client']

    formatted_names = list(map(shell.myformat_colname, names))
    formatted_values = [list(map(shell.myformat_value, row)) for row in rows]

    shell.writeresult('')
    shell.writeresult('Tracing session: ', color=MAGENTA, newline=False)
    shell.writeresult(trace.trace_id)
    shell.writeresult('')
    shell.print_formatted_result(formatted_names, formatted_values, with_header=True, tty=shell.tty)
    shell.writeresult('')

    if temp_query is not None:
        shell.query_out = temp_query
        shell.color = temp_color


def make_trace_rows(trace):
    if not trace.events:
        return []

    rows = [[trace.request_type, str(datetime_from_utc_to_local(trace.started_at)), trace.coordinator, 0, trace.client]]

    # append main rows (from events table).
    for event in trace.events:
        rows.append(["%s [%s]" % (event.description, event.thread_name),
                     str(datetime_from_utc_to_local(event.datetime)),
                     event.source,
                     total_micro_seconds(event.source_elapsed),
                     trace.client])
    # append footer row (from sessions table).
    if trace.duration:
        finished_at = (datetime_from_utc_to_local(trace.started_at) + trace.duration)
        rows.append(['Request complete', str(finished_at), trace.coordinator, total_micro_seconds(trace.duration), trace.client])

    return rows


def total_micro_seconds(td):
    """
    Convert a timedelta into total microseconds
    """
    return int((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6)) if td else "--"


def datetime_from_utc_to_local(utc_datetime):
    """
    Convert a naive UTC datetime to the local timezone.
    This is necessary because the driver always returns naive datetime objects.
    """
    return utc_datetime.replace(tzinfo=timezone.utc).astimezone()
