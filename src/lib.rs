use std::{collections::BTreeMap, usize};

#[derive(Debug, PartialEq, Clone)]
pub enum Type {
    Str,
    Int,
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Value {
    Str(String),
    Int(i64),
}

pub type Row = Vec<Value>;
// have a row type as an iterable, inspired by toydb

#[derive(Debug, PartialEq, Clone)]
pub struct Attribute {
    name: String,
    atype: Type,
}

#[derive(Debug, Clone)]
pub struct Schema {
    attributes: Vec<Attribute>,
}

impl PartialEq<Value> for Type {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Type::Str, Value::Str(_)) => true,
            (Type::Int, Value::Int(_)) => true,
            _ => false,
        }
    }
}

impl Schema {
    pub fn validate_row(&self, row: &Row) -> bool {
        if row.len() != self.attributes.len() {
            return false;
        }

        let res = self
            .attributes
            .iter()
            .zip(row.iter())
            .all(|(a, b)| a.atype == *b);

        return res;
    }
}

#[derive(Debug, Clone)]
pub enum Data {
    WithPK(Box<BTreeMap<Value, Row>>),
    NoPK((i32, Box<BTreeMap<i32, Row>>)),
}

impl Data {
    pub fn insert(&mut self, key: Option<Value>, row: Row) -> bool {
        match self {
            Data::WithPK(tree) => {
                if key.is_none() {
                    println!("[PANIC] key not provided while inserting BTree");
                    return false;
                }

                _ = tree.insert(key.unwrap(), row);
                return true;
            }
            Data::NoPK((key, tree)) => {
                tree.insert(*key, row);
                *key += 1;
                return true;
            }
        }
    }

    pub fn contains(&self, key: Option<Value>, row: Option<Row>) -> bool {
        match self {
            Data::WithPK(tree) => {
                if key.is_none() {
                    println!("[DEBUG] {:?}", &self);
                    println!("[PANIC] key not provided while querying BTree");
                    return false;
                }
                tree.contains_key(&key.unwrap())
            }
            Data::NoPK((_, tree)) => {
                if row.is_none() {
                    println!("[DEBUG] {:?}", &self);
                    println!("[PANIC] row not provided while querying BTree");
                    return false;
                }
                tree.values().any(|v| v == row.as_ref().unwrap())
            }
        }
    }

    pub fn tuples(&self) -> Vec<Row> {
        match self {
            Data::WithPK(tree) => Vec::from_iter(tree.values())
                .into_iter()
                .map(|inner| inner.clone())
                .collect(),
            Data::NoPK((_, tree)) => Vec::from_iter(tree.values())
                .into_iter()
                .map(|inner| inner.clone())
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct Relation {
    name: String,
    pk: Option<usize>,
    // fks: Option<Vec<usize>>,
    schema: Schema,

    // data: BTreeMap<Value, Row>,
    data: Data,
}

impl Relation {
    pub fn insert_row(&mut self, row: Row) -> bool {
        if !self.schema.validate_row(&row) {
            return false;
        }

        if self.pk.is_none() {
            // insert regardless
            return self.data.insert(None, row);
        }

        if self
            .data
            .contains(Some(row[self.pk.unwrap()].clone()), None)
        {
            return false;
        }

        _ = self.data.insert(Some(row[self.pk.unwrap()].clone()), row);

        true
    }

    pub fn insert_rows(&mut self, rows: Vec<Row>) -> bool {
        if !rows.iter().all(|r| self.schema.validate_row(r)) {
            println!("[ERROR] rows are not valid, not inserting - INSERT ROWS");
            return false;
        }

        if self.pk.is_none() {
            // insert all rows even if there are duplicates
            for row in rows {
                _ = self.data.insert(None, row);
            }
            return true;
        }

        // rows if dup because of primary key repeations
        let nondup_rows = rows
            .iter()
            .map(|r| r[self.pk.unwrap()].clone())
            .collect::<std::collections::HashSet<Value>>()
            .len();

        if nondup_rows != rows.len() {
            println!(
                "[ERROR] there are repeats in the primary key used, not inserting - INSERT ROWS"
            );
            return false;
        }

        let new_data = rows
            .iter()
            .all(|r| self.data.contains(Some(r[self.pk.unwrap()].clone()), None) == false);
        if !new_data {
            println!("[DEBUG] {:?}", &self);
            println!("[ERROR] insert rows failed - INSERT ROWS");
            return false;
        }

        for row in rows {
            _ = self.data.insert(Some(row[self.pk.unwrap()].clone()), row);
        }

        return true;
    }

    // this is being used in tests
    // #[allow(dead_code)]
    // fn get_tuples(&self) -> Vec<Row> {
    //     Vec::from_iter(self.data.values())
    //         .into_iter()
    //         .map(|inner| inner.clone())
    //         .collect::<Vec<Row>>()
    // }
}

#[derive(Debug)]
pub enum Comp {
    /// Greater than
    GT,
    /// Lesser than
    LT,
    /// Greater than or Equal To
    GE,
    /// Lesser than or Equal To
    LE,
    /// Equal To
    EQ,
    /// Not-Equal To
    NE,
}

#[derive(Debug)]
pub enum Connective {
    /// Conjunction
    AND,
    /// Disjunction
    OR,
}

#[derive(Debug)]
pub enum SelPredicate {
    Condition(
        (Attribute, Comp, Value),
        Option<(Connective, Box<SelPredicate>)>,
    ),
    None,
}

impl SelPredicate {
    pub fn validate(&self) -> bool {
        match self {
            SelPredicate::Condition((_, _, _), _) => {
                unimplemented!()
            }
            SelPredicate::None => {
                // this is equivalent to
            }
        }

        return false;
    }
}

#[derive(Debug)]
pub enum ProjAttrs {
    Attr(Attribute, Option<Box<ProjAttrs>>),
    None,
}

struct ProjAttrIterator<'a> {
    current: &'a ProjAttrs,
}

impl ProjAttrs {
    fn iter(&self) -> ProjAttrIterator<'_> {
        ProjAttrIterator { current: self }
    }

    pub fn execute(&self, relation: &Relation) -> Option<Relation> {
        // println!("[Projection] query {:?}", self);
        match self {
            ProjAttrs::None => {
                // Same as SELECT * FROM relation
                println!("[DEBUG][Projection] Query : Select *, returning all tuples");
                let values = relation
                    .data
                    .tuples()
                    .into_iter()
                    .collect::<std::collections::HashSet<Row>>()
                    .iter()
                    .map(|row| row.clone())
                    .collect::<Vec<Row>>();

                let mut derived = Relation {
                    name: "derived".to_string(),
                    pk: relation.pk,
                    schema: relation.schema.clone(),
                    data: {
                        if relation.pk.is_some() {
                            Data::WithPK(Box::new(BTreeMap::new()))
                        } else {
                            Data::NoPK((0, Box::new(BTreeMap::new())))
                        }
                    },
                };

                derived.insert_rows(values);

                return Some(derived);
            }
            _ => {}
        }

        let satisfied = self.iter().all(|a| relation.schema.attributes.contains(a));
        if !satisfied {
            println!(
                "[ERROR][Projection] selected attributes {:?} dont exist",
                &self
            );
            return None;
        }

        println!(
            "[DEBUG][Projection] QUERY : SELECT attributes {:?}, returning tuples",
            &self
        );

        let rel_attributes = self.iter().map(|f| f.clone()).collect::<Vec<_>>();

        let selected_attrs_indices = self
            .iter()
            .map(|a| relation.schema.attributes.iter().position(|x| x == a))
            .map(|i| i.unwrap())
            .collect::<Vec<_>>();

        let mut pk_missing = false;
        if relation.pk.is_some() & !selected_attrs_indices.contains(&relation.pk.unwrap()) {
            println!(
                "[Projection] {:?} PK is not in the selected attributes",
                &relation.pk
            );
            pk_missing = true;
        }

        let values = relation
            .data
            .tuples()
            .into_iter()
            .map(|inner| {
                inner
                    .clone()
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| selected_attrs_indices.contains(index))
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::HashSet<Row>>() // remove the duplicates
            .iter()
            .map(|v| v.clone())
            .collect::<Vec<_>>();

        if pk_missing {
            let mut derived = Relation {
                name: "derived".to_string(),
                pk: None,
                schema: Schema {
                    attributes: rel_attributes,
                },
                data: Data::NoPK((0, Box::new(BTreeMap::new()))),
            };

            derived.insert_rows(values);
            return Some(derived);
        }

        let mut derived = Relation {
            name: "derived".to_string(),
            pk: relation.pk,
            schema: Schema {
                attributes: rel_attributes,
            },
            data: Data::WithPK(Box::new(BTreeMap::new())),
        };

        derived.insert_rows(values);

        return Some(derived);
    }
}

impl<'a> Iterator for ProjAttrIterator<'a> {
    type Item = &'a Attribute;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            ProjAttrs::Attr(a, next) => {
                if let Some(n) = next {
                    self.current = n
                } else {
                    self.current = &ProjAttrs::None
                }
                Some(a)
            }
            ProjAttrs::None => None,
        }
    }
}

// #[derive(Debug)]
// pub type Predicates

// #[derive(Debug)]
// pub struct Predicate {
//     attribute: Attribute,
// }

#[derive(Debug)]
pub enum UnaryOpr<'a> {
    Selection(SelPredicate, &'a Relation),
    Projection(ProjAttrs, &'a Relation),
}

impl UnaryOpr<'_> {
    pub fn evaluate(&self) -> Option<Relation> {
        match self {
            UnaryOpr::Projection(p, r) => {
                return p.execute(*r);
            }
            UnaryOpr::Selection(_, _) => {
                return None;
            }
        };
    }
}

#[derive(Debug)]
pub enum BinaryOpr {}

#[derive(Debug)]
pub enum Operator<'a> {
    Unary(UnaryOpr<'a>),
    Binary(BinaryOpr),
}

impl Operator<'_> {
    pub fn evaluate(&self) -> Option<Relation> {
        match self {
            Operator::Unary(opr) => {
                return opr.evaluate();
            }
            Operator::Binary(_) => {
                return None;
            }
        };
    }
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn create_test_schema() -> Schema {
        let schema = Schema {
            attributes: vec![
                Attribute {
                    name: "key".to_string(),
                    atype: Type::Int,
                },
                Attribute {
                    name: "value".to_string(),
                    atype: Type::Str,
                },
            ],
        };

        schema
    }

    fn create_test_relation() -> Relation {
        let schema = Schema {
            attributes: vec![
                Attribute {
                    name: "key".to_string(),
                    atype: Type::Int,
                },
                Attribute {
                    name: "value".to_string(),
                    atype: Type::Str,
                },
            ],
        };

        let relation = Relation {
            name: "test".to_string(),
            pk: Some(0),
            // fks: None,
            schema,
            data: Data::WithPK(Box::new(BTreeMap::new())),
        };

        relation
    }

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn validate_invalid_row_schema() {
        let schema = create_test_schema();

        assert_eq!(
            schema.validate_row(&vec![Value::Str("foo".to_string()), Value::Int(1)]),
            false
        )
    }

    #[test]
    fn validate_row_schema() {
        let schema = create_test_schema();

        assert_eq!(
            schema.validate_row(&vec![Value::Int(1), Value::Str("foo".to_string())]),
            true
        )
    }

    #[test]
    fn test_insert_row() {
        let mut relation = create_test_relation();

        assert_eq!(
            relation.insert_row(vec![Value::Int(1), Value::Str("foo".to_string())]),
            true
        );

        println!("[TEST] data inserted: {:?}", &relation.data);

        assert_eq!(
            relation.insert_row(vec![Value::Int(1), Value::Str("bar".to_string())]),
            false
        );

        println!("[TEST] duplicate row not inserted");

        assert_eq!(
            relation.insert_rows(vec![
                vec![Value::Int(2), Value::Str("foo".to_string())],
                vec![Value::Int(3), Value::Str("bar".to_string())],
            ]),
            true
        );

        println!("[TEST] multiple inserts {:?}", &relation.data);

        assert_eq!(
            relation.insert_rows(vec![
                vec![Value::Int(1), Value::Str("foo".to_string())],
                vec![Value::Int(2), Value::Str("bar".to_string())],
                vec![Value::Int(3), Value::Str("baz".to_string())],
            ]),
            false
        );

        println!("[TEST] not inserting rows if duplicates found");

        assert_eq!(
            relation.insert_rows(vec![
                vec![Value::Int(4), Value::Str("apple".to_string())],
                vec![Value::Int(5), Value::Str("orange".to_string())],
                vec![Value::Int(6), Value::Str("orange".to_string())],
            ]),
            true
        );

        println!("[TEST] multiple inserts {:?}", &relation.data);
    }

    #[test]
    fn basic_projections() {
        let mut relation = create_test_relation();

        relation.insert_rows(vec![
            vec![Value::Int(1), Value::Str("foo".to_string())],
            vec![Value::Int(2), Value::Str("bar".to_string())],
            vec![Value::Int(3), Value::Str("baz".to_string())],
        ]);

        let select_all = Operator::Unary(UnaryOpr::Projection(ProjAttrs::None, &relation));
        let result = select_all.evaluate();

        assert_eq!(result.is_some(), true);

        assert_eq!(
            result.as_ref().unwrap().data.tuples(),
            vec![
                vec![Value::Int(1), Value::Str("foo".to_string())],
                vec![Value::Int(2), Value::Str("bar".to_string())],
                vec![Value::Int(3), Value::Str("baz".to_string())],
            ]
        );

        println!("[TEST] query result: {:?}", result.unwrap());

        let select_value_attr = Operator::Unary(UnaryOpr::Projection(
            ProjAttrs::Attr(
                Attribute {
                    name: "value".to_string(),
                    atype: Type::Str,
                },
                None,
            ),
            &relation,
        ));

        let result = select_value_attr.evaluate();
        assert_eq!(result.is_some(), true);
        let mut left = result.as_ref().unwrap().data.tuples();
        let mut right = vec![
            vec![Value::Str("foo".to_string())],
            vec![Value::Str("bar".to_string())],
            vec![Value::Str("baz".to_string())],
        ];

        left.sort();
        right.sort();
        assert_eq!(left, right);

        println!("[TEST] selecting a single attribute {:?}", result);
    }

    #[test]
    fn test_remove_duplicates() {
        let mut relation = Relation {
            name: "pk_less".to_string(),
            pk: Some(0),
            schema: Schema {
                attributes: vec![
                    Attribute {
                        name: "id".to_string(),
                        atype: Type::Int,
                    },
                    Attribute {
                        name: "value".to_string(),
                        atype: Type::Str,
                    },
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

        println!("[test] Project removed duplicate tuples");

        let derived = result.unwrap();
        assert!(derived.pk.is_none());

        let derived_query = Operator::Unary(UnaryOpr::Projection(ProjAttrs::None, &derived));
        let derived_query_result = derived_query.evaluate();

        assert!(derived_query_result.is_some());

        let mut left = derived_query_result.as_ref().unwrap().data.tuples();
        let mut right = vec![
            vec![Value::Str("foo".to_string())],
            vec![Value::Str("bar".to_string())],
            vec![Value::Str("baz".to_string())],
        ];

        left.sort();
        right.sort();
        assert_eq!(left, right);
        assert!(derived_query_result.unwrap().pk.is_none());

        println!("[test] Project removed duplicate tuples");
    }

    #[test]
    fn test_user_schema() {
        // tbl users
        // | id INT PK | name STR | phone INT
        let mut relation = Relation {
            name: "users".to_string(),
            pk: Some(0),
            schema: Schema {
                attributes: vec![
                    Attribute {
                        name: "id".to_string(),
                        atype: Type::Int,
                    },
                    Attribute {
                        name: "name".to_string(),
                        atype: Type::Str,
                    },
                    Attribute {
                        name: "phone".to_string(),
                        atype: Type::Int,
                    },
                ],
            },
            data: Data::WithPK(Box::new(BTreeMap::new())),
        };

        // 100 | bob | 9999999999
        // 101 | alice | 6666666666
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

        // pi_{name, phone}
        let query = Operator::Unary(UnaryOpr::Projection(
            ProjAttrs::Attr(
                Attribute {
                    name: "name".to_string(),
                    atype: Type::Str,
                },
                Some(Box::new(ProjAttrs::Attr(
                    Attribute {
                        name: "phone".to_string(),
                        atype: Type::Int,
                    },
                    None,
                ))),
            ),
            &relation,
        ));

        let result = query.evaluate();

        // tbl derived
        // bob | 9999999999
        // alice | 6666666666
        let mut left = result.as_ref().unwrap().data.tuples();
        let mut right = vec![
            vec![Value::Str("bob".to_string()), Value::Int(9999999999)],
            vec![Value::Str("alice".to_string()), Value::Int(6666666666)],
        ];

        left.sort();
        right.sort();
        assert_eq!(left, right);
    }
}
