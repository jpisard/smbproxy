# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

from twisted.enterprise import adbapi


class AuditLogger(object):
    def __init__(self, settings):
        self.settings = settings
        self.dbpool = adbapi.ConnectionPool(
            'psycopg2',
            host=self.settings.AUDIT_LOG_HOST,
            database='smbproxy4',
            user='smbproxy4',
            password='smbproxy4',
            cp_reconnect=True,
        )

    def log(self, log_context, start_time, duration_in_ms, status):
        if not self.settings.ENABLE_AUDIT_LOG:
            return

        # The 'RETURNING' clause is necessary. Without it, psycopg2 doesn't return anything, and runQuery fails.
        query = 'INSERT INTO File_operations (' \
                'share_name, path, client_host, operation_type, start_time, duration_in_ms, status' \
                ') VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING ID'

        params = (
            log_context._context['share_name'],
            log_context._context['path'],
            log_context._context['peer'],
            log_context._context['action_type'],
            start_time.isoformat(),
            duration_in_ms,
            status
        )

        return self.dbpool.runQuery(query, params)

