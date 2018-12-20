"""
Playing around with the OO API
"""
from splitgraph.core.repository import Repository


def test_oo_api(local_engine_with_pg_and_mg):
    # Repository just contains the engine and its name (doesn't
    # load anything, any API call results in a query).

    pg = Repository('test', 'pg_mount')
    all_image_hashes = pg.get_images()
    assert len(all_image_hashes) == 1

    # Engine is inside the repository -- currently this doesn't happen, but the engine
    # should affect which queries the metadata hit so that I can do e.g.
    # remote = Repository('noaa/climate', engine='splitgraph.com')
    # if '20181227' in remote.get_tags():  # hits the splitgraph.com engine
    #     print("data released!")
    # Repository('my/climate').pull(remote)
    assert pg.engine == local_engine_with_pg_and_mg

    image_1_obj = pg.get_image(all_image_hashes[0])
    # Image is currently based on a Namedtuple with the same names as the columns.
    assert image_1_obj.image_hash == all_image_hashes[0]
    assert image_1_obj.parent_id is None

    # Mostly operating on image hashes (not Image objects) still
    image_1 = image_1_obj.image_hash
    pg.checkout(image_1)
    assert pg.get_head() == image_1

    # run_sql executed inside of the schema
    assert pg.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange')]
    pg.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")

    # should diff() be a repository method or an image method? we can't diff (efficiently) between
    # two images not in the same repository so there's no point in calling
    # image_from_one_repo.diff(image_from_other_repo).
    assert pg.diff('fruits', image_1, None, aggregate=True) == (1, 0, 0)
    assert pg.diff('fruits', image_1, None, aggregate=False) == [((3, 'mayonnaise'), 0, {'c': [], 'v': []})]

    image_2 = pg.commit()

    assert pg.diff('fruits', image_1, image_2, aggregate=True) == (1, 0, 0)
    assert pg.diff('fruits', image_1, image_2, aggregate=False) == [((3, 'mayonnaise'), 0, {'c': [], 'v': []})]

    image_2_obj = pg.get_image(image_2)
    assert image_2_obj.parent_id == image_1
    tables = image_2_obj.get_tables()
    assert tables == ['fruits', 'vegetables']
    fruits = image_2_obj.get_table('fruits')

    assert len(fruits.objects) == 1
    assert fruits.objects[0][1] == 'DIFF'

    # again this just queries the DB directly -- no caching or making sure
    # the internal state of image_2_obj is the same as the database + no need to do image_2_obj.save()
    assert image_2_obj.get_tags() == ['HEAD']
    image_2_obj.tag('test_tag')
    assert image_2_obj.get_tags() == ['HEAD', 'test_tag']
