from misc import prometheus as p, openstack_helpers as oshelp


def test_init():
    p.init("secrets.yml")
    assert p.q("1", 'eu')['status'] == 'success'
    assert p.q("42", 'eu')['data']['result'][1] == '42'
    assert p.q("1", 'us')['status'] == 'success'
