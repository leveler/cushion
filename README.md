
# couch cushion

Minimalist Couchbase object wrapper for python.

- uses couchbase official driver
- tested
- fast
- could be modified to work with other data persistence layers as needed with
  minimal effort

# note about needed libraries

You will need to install libcouchbase and python couchbase external to this
library.  They are not set as dependencies but will be considered to be
satisfied externally if you intend to use the CouchbaseConnection.

# Basic Usage

Connect to a persistence layer, then work through your models.

## connect at the module level

This favors convenience.

```python
from cushion.persist import set_connection
from cushion.persist.cb import CouchbaseConnection
from cushion.model import Model, DocTypeMismatch, DocTypeNotFound
from cushion.field import Field

# do this one time, before any db related work
set_connection(CouchbaseConnection('lvlrtest', 'localhost', 'gogogogo'))
```

## simple models

```python
class SomeModel(Model):
  myfield = Field(default="shoes")
```

This model has one generic field with a default value that can be overwritten
as needed.

## instantiating

Models can be instantiated blankly:

```python
some_one = SomeModel()
```

Or, models can be instantiated with values as needed:

```python
some_one = SomeModel(myfield="other value")
```

## manipulating

Once you have a model, you can assign directly to the members.

```python
some_one.myfield = "cool stuff"
```

## saving

Persisting is easy.

```python
some_one.save()
```

Saves return the instance they have saved, so it's simple to chain
instantiation and creation.

```python
some_one = SomeModel(myfield="other value").save()
```

## deleting

Deleting is easy also.

```python
some_one.delete()
```

## retrieving from persistence

After a document is saved, it has an `id` field defined.

```python
some_one = SomeModel().save()
my_doc_id = some_one.id
```

If you have the `id` you can retrieve that document directly.

```python
original_one = SomeModel.load(my_doc_id)
```

# Model Fields

There are many field types that provide some validation and ease.  Some
examples are `TextField`, `FloatField`, etc.

## scalar fields

```python
from cushion.field import DateTimeField, IntegerField, TextField

class Pants(Model):
  size = IntegerField(default=32)
  color = TextField(default="blue")
  inspected_date = DateTimeField(default=datetime.now)
```

Notice the `default` parameter above for the `inspected_date` is not a scalar
but a callable.  The function will be executed when the value is accessed the
first time.


## reference fields

You can also reference another `Model` using a `RefField`.

```python
from cushion.field import RefField

class Outfit(Model):
    last_worn = DateTimeField()
    pants = RefField(Pants)

blue_pants = Pants().save()
monday_attire = Outfit(last_worn=datetime.now(), pants=blue_pants).save()
```

You can then access the referenced model simply:

```python
print "My pants were {}".format( monday_attire.pants.color )
```

## collection fields

Collection fields are limited to basic python types only right now.

```python
from cushion.field import ListField, DictField

class AnotherThing(Model):
    my_stuff = ListField()
    my_dict = DictField()

tt = AnotherThing()
tt.my_stuff.append('this one time')
tt.my_dict['mykey'] = 'bigfoot'
```


**todo** add more documentation on field types

## Queries can be performed once a view is defined

By defining a view, and then adding a helper method to your `Model`, you can
simplify accessing the docs by params.

```python
from cushion.view import View, sync_all

class Shoe(Model):
    size = IntegerField()
    by_size = View(
      'shoes', 'by_size',
      '''
      function(doc, meta) {
        if (doc.type=='shoe') {
          emit(doc.size, null)
        }
      }
      ''' )

    @classmethod
    def all_for_size(cls, sz):
      res = cls.by_size(startkey=sz, endkey=sz, include_docs=True)
      return res


# be sure to sync your views before using them
sync_all(Shoe.viewlist())

# now have a list of Shoe objects that are size 11
size_11_shoes = Shoe.all_for_size(11)
```

# MemConnection

There is a mock connection type, called a `MemConnection`, that allows you to
test your models w/o needing a live couchbase instance.

To use it instead of the couchbase connection, just set your active connection
to a MemConnection instance.

```
from cushion.persist.mem import MemConnection
set_connection(MemConnection())
```

# Tests

To run tests, do the following:

- chdir to the cushion base directory: `cd cushion`
- initiate a virtualenv: `mkdir .venv && virtualenv .venv`
- activate the virtualenv: `source .venv/bin/activate`
- install runtime requirements: `pip install -r requirements.txt`
- install dev requirements: `pip install -r requirements_test.txt`
- finally, run the tests: `nosetests`

```
cushion$ nosetests
............
----------------------------------------------------------------------
Ran 12 tests in 0.770s

OK
```

# TODOS

- Unit tests - all functional for now, sorry.


# THANKS to our forebears!

Cushion is heavily inspired by [mogo](https://github.com/joshmarshall/mogo).

We also learned a few lessons from
[couchbase-mapping](https://github.com/hdmessaging/couchbase-mapping-python).

