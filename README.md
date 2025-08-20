# Codd

**motivation**

Named after EF Codd after the paper [A Relational Model of Data for Large Shared Data Banks](https://www.seas.upenn.edu/~zives/03f/cis550/codd.pdf). The current project is just a single file POC trying to get relational algebra to work. There are no optimisations.

I do have plans to build upon this for a toy database implementation.

**references**

- [erikgrinaker/toydb](https://github.com/erikgrinaker/toydb) was a good reference especially from the AST to Executor phase for schema creation

**example**

Here is an example of a simple users relation, with columns (id INT PK, name STR, phone STR). The test performs a projection selecting only the name and phone attributes and produces a derived relation.

```rust
#[test]
fn test_user_schema() {
    // tbl users
    // schema: id INT PK | name STR | phone INT
    let mut relation = Relation {
        name: "users".to_string(),
        pk: 0,
        schema: Schema {
            attributes: vec![
                Attribute { name: "id".to_string(), atype: Type::Int },
                Attribute { name: "name".to_string(), atype: Type::Str },
                Attribute { name: "phone".to_string(), atype: Type::Int },
            ],
        },
        // This is going to change in the future
        data: Data::WithPK(Box::new(BTreeMap::new())),
    };

    // id   | name      | phone
    // 100  | bob       | 9999999999
    // 101  | alice     | 6666666666
    let insert_result = relation.insert_rows(vec![
        vec![
            Value::Int(100),
            Value::Str("bob".to_string()),
            Value::Int(9999999999),
        ],
        vec![
            Value::Int(101),
            Value::Str("alice".to_string()),
            Value::Int(6666666666),
        ],
    ]);
    assert!(insert_result);
    println!("{:?}", relation.data);

    // π_{name, phone} (users) ≡ SELECT name, phone FROM users
    let query = Operator::Unary(UnaryOpr::Projection(
        ProjAttrs::Attr(
            Attribute { name: "name".to_string(), atype: Type::Str },
            Some(Box::new(ProjAttrs::Attr(
                Attribute { name: "phone".to_string(), atype: Type::Int },
                None,
            ))),
        ),
        &relation,
    ));

    let result = query.evaluate();

    // tbl derived
    // name  | phone
    // bob   | 9999999999
    // alice | 6666666666
    let mut left = result.as_ref().unwrap().data.tuples();
    let mut right = vec![
        vec![Value::Str("bob".to_string()), Value::Int(9999999999)],
        vec![Value::Str("alice".to_string()), Value::Int(6666666666)],
    ];

    left.sort();
    right.sort();
    assert_eq!(left, right);

    println!("{:?}", result.unwrap());
}
```

Example where projection removes duplicate tuples

```rust
#[test]
fn test_remove_duplicates() {
    let mut relation = Relation {
        name: "pk_less".to_string(),
        pk: Some(0),
        schema: Schema {
            attributes: vec![
                Attribute { name: "id".to_string(), atype: Type::Int },
                Attribute { name: "value".to_string(), atype: Type::Str },
            ],
        },
        data: Data::WithPK(Box::new(BTreeMap::new())),
    };

    let insert_result = relation.insert_rows(vec![
        vec![Value::Int(1), Value::Str("foo".to_string())],
        vec![Value::Int(2), Value::Str("bar".to_string())],
        vec![Value::Int(3), Value::Str("baz".to_string())],
        vec![Value::Int(4), Value::Str("foo".to_string())],
    ]);

    assert!(insert_result);
    assert_eq!(
        relation.data.tuples(),
        vec![
            vec![Value::Int(1), Value::Str("foo".to_string())],
            vec![Value::Int(2), Value::Str("bar".to_string())],
            vec![Value::Int(3), Value::Str("baz".to_string())],
            vec![Value::Int(4), Value::Str("foo".to_string())],
        ]
    );

    let query = Operator::Unary(UnaryOpr::Projection(
        ProjAttrs::Attr(
            Attribute {
                name: "value".to_string(),
                atype: Type::Str,
            },
            None,
        ),
        &relation,
    ));
    let result = query.evaluate();
    // result here is a new relation named derived with data
    // defiend by
    assert!(result.is_some());

    let mut left = result.as_ref().unwrap().data.tuples();
    let mut right = vec![
        vec![Value::Str("foo".to_string())],
        vec![Value::Str("bar".to_string())],
        vec![Value::Str("baz".to_string())],
    ];

    left.sort();
    right.sort();
    assert_eq!(left, right);
}
```

You can query on derived queries as well

```rust
// tbl users
// id INT PK  | name STR  | phone INT
// 100        | bob       | 9999999999
// 101        | alice     | 6666666666

// SELECT name, phone FROM users;
let query = Operator::Unary(UnaryOpr::Projection(
    ProjAttrs::Attr(
        Attribute { name: "name".to_string(), atype: Type::Str },
        Some(Box::new(ProjAttrs::Attr(
            Attribute { name: "phone".to_string(), atype: Type::Int },
            None,
        ))),
    ),
    &users,
)).evaluate();

let result = query.evaluate();
assert!(result.is_some());
let derived = result.unwrap();
// tbl derived
// name STR  | phone INT
// bob       | 9999999999
// alice     | 6666666666

// SELECT phone FROM (
//  SELECT name, phone FROM users
// );
let query = Operator::Unary(UnaryOpr::Projection(
    ProjAttrs::Attr(
        Attribute { name: "phone".to_string(), atype: Type::Int },
        None,
    ),
    &derived,
)).evaluate();

let result = query.evaluate();
assert!(result.is_some());
let derived = result.unwrap();
// tbl derived
// phone INT
// 9999999999
// 6666666666
```
