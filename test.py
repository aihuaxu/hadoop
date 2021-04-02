def parse_host(host):
    # parse hadoopchlorinenamenode01-phx2.prod.uber.internal:8020
    if 'namenode' in host or 'observer' in host:
        return host.split('-')[0].replace('hadoop', '')\
            .replace('namenode', '').replace('observer', 'o')

    return host[0:6]

print parse_host('hadoopplatinumnamenode01-phx2.prod.uber.internal:50070')
print parse_host('hadoopplatinumobserver01-phx2.prod.uber.internal:50070')
print parse_host('hadoopworker01-phx2.prod.uber.internal:50070')
print parse_host('hadoopstorage01-phx2.prod.uber.internal:50070')
