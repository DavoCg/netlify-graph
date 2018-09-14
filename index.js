const {ApolloServer, ForbiddenError, gql} = require('apollo-server');
const DataLoader = require('dataloader');
const keyBy = require('lodash/keyBy');
const groupBy = require('lodash/groupBy');
const squel = require('squel').useFlavour('postgres');
const pgp = require('pg-promise')();

const db = pgp({
  host: 'localhost',
  port: 5432,
  user: '',
  database: 'goodwatch',
  password: ''
});

// QUERIES
const getSignals = () => {
  return squel.select()
    .from('signals')
    .toString();
};

const getUsers = () => {
  return squel.select()
    .from('users')
    .toString();
};

const getGoodsByIds = ({ids}) => {
  return squel.select()
    .from('goods')
    .where('id IN ?', ids)
    .toString();
};

const getUsersGoods = ({ids}) => {
  return squel.select()
    .from('users_goods')
    .field('users_goods.user_id')
    .field('goods.*')
    .join('goods', null, 'goods.id = users_goods.good_id')
    .where('users_goods.user_id IN ?', ids)
    .toString();
};

const addUser = ({phone}) => {
  return squel.insert()
    .set('phone', phone)
    .into('users')
    .returning('*')
    .toString();
};

// LOADERS
const getLoaders = ({db}) => ({
  goodLoader: createGoodLoader({db}),
  goodsLoader: createGoodsLoader({db}),
});

const createGoodLoader = ({db}) => new DataLoader(async ids => {
  const goods = await db.manyOrNone(getGoodsByIds({ids}));
  const goodsById = keyBy(goods, 'id');
  return ids.map(id => goodsById[id]);
});

const createGoodsLoader = ({db}) => new DataLoader(async ids => {
  const usersGoods = await db.manyOrNone(getUsersGoods({ids}));
  const usersGoodsById = groupBy(usersGoods, 'user_id');
  return ids.map(id => usersGoodsById[id]);
});

// DEFS
const typeDefs = gql`
  type User {
    id: Int
    phone: String,
    goods: [Good]
  }
  type Good {
    id: Int
    identification: String
  }
  type Signal {
    id: Int
    good: Good
  }
  type Query {
    signals: [Signal]
    users: [User]
  }
  type Mutation {
    addUser (
      phone: String!
    ): User
  }
`;

// RESOLVERS
const resolvers = {
  Mutation: {
    addUser: async (parents, args) => await db.one(addUser(args)),
  },
  Query: {
    signals: async (parents, args) => await db.manyOrNone(getSignals(args)),
    users: async (parents, args) => await db.manyOrNone(getUsers(args)),
  },

  Signal: {
    good: async (parent, args, context) => context.loaders.goodLoader.load(parent.good_id)
  },

  User: {
    goods: async (parent, args, context) => context.loaders.goodsLoader.load(parent.id)
  },
};

const context = ({req}) => {
  const token = req.headers.authorization;
  const user = token === '123abcde' ? {id: '123'} : {};
  if(!user) throw new ForbiddenError('You need to be authenticated to access this schema');
  return {
    user: user,
    loaders: getLoaders({db})
  };
};

const server = new ApolloServer({typeDefs, resolvers, context, tracing: true});



const logger = data => console.log('data', data);

server.listen().then(({url}) => {
  console.log(`🚀  Server ready at ${url}`);
});