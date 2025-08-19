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
        data: BTreeMap::new(),
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
    // PKID | name  | phone
    // 0    | bob   | 9999999999
    // 1    | alice | 6666666666
    // ^^^^
    // the new column here is a PK for
    // the derived relation
    assert_eq!(
        result.as_ref().unwrap().get_tuples(),
        vec![
            vec![Value::Int(0), Value::Str("bob".to_string()), Value::Int(9999999999)],
            vec![Value::Int(1), Value::Str("alice".to_string()), Value::Int(6666666666)],
        ]
    );
}
```
