package com.javarush;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javarush.dao.CityDAO;
import com.javarush.dao.CountryDAO;
import com.javarush.domain.City;
import com.javarush.domain.Country;
import com.javarush.domain.CountryLanguage;
import com.javarush.redis.CityCountry;
import com.javarush.redis.Language;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import java.util.ArrayList;
import java.util.Properties;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;
    private final ObjectMapper mapper;
    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public Main() {

        sessionFactory = prepareRelationalDB();
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);
        redisClient = prepareRedisClient();
        mapper = new ObjectMapper();
    }

    private RedisClient prepareRedisClient() {
        RedisClient redisClient = RedisClient.create(RedisURI.create("localhost",6379));
        try(StatefulRedisConnection<String,String> connection = redisClient.connect()) {
            System.out.println("\nConnected to Redis\n");
        }
        return redisClient;
    }

    private SessionFactory prepareRelationalDB() {
        final SessionFactory sessionFactory;
        Properties properties = new Properties();
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");

        sessionFactory = new Configuration()
                .setProperties(properties)
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();

        return sessionFactory;
    }

    private void shutDown() {
        if(nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if(nonNull(redisClient)) {
            redisClient.close();
        }
    }

    private List<City> fetchData(Main run) {
            try(Session session = run.sessionFactory.getCurrentSession()){
                List<City> allCities = new ArrayList<>();
                session.beginTransaction();
                List<Country> countries = run.countryDAO.getAll();
                int totalCount = run.cityDAO.getTotalCount();
                int step = 500;
                for (int i = 0; i < totalCount; i+=step) {
                     allCities.addAll(run.cityDAO.getItems(i,step));
                }
                session.getTransaction().commit();
                return allCities;
            }
    }

    private List<CityCountry> transformData(List<City> cities) {
            return cities.stream().map(city -> {
                CityCountry res = new CityCountry();
                res.setId(city.getId());
                res.setName(city.getName());
                res.setPopulation(city.getPopulation());
                res.setDistrict(city.getDistrict());

                Country country = city.getCountry();
                res.setAlternativeCountryCode(country.getAnotherCode());
                res.setContinent(country.getContinent());
                res.setCountryCode(country.getCode());
                res.setCountryName(country.getName());
                res.setPopulation(country.getPopulation());
                res.setCountrySurfaceArea(country.getSurfaceArea());
                res.setCountryRegion(country.getRegion());
                Set<CountryLanguage> countryLanguages = country.getLanguages();
                Set<Language> languages = countryLanguages.stream().map(cl ->{
                    Language language = new Language();
                    language.setLanguage(cl.getLanguage());
                    language.setOfficial(cl.getOfficial());
                    language.setPercentage(cl.getPercentage());
                    return language;
                }).collect(Collectors.toSet());
                res.setLanguages(languages);

                return res;
                    }).collect(Collectors.toList());
    }

    private void pushToRedis(List<CityCountry> data) {
       try(StatefulRedisConnection<String,String> connection = redisClient.connect()) {
           RedisStringCommands<String,String> sync = connection.sync();
           for (CityCountry cityCountry : data) {
               try{
                   sync.set(String.valueOf(cityCountry.getId()),mapper.writeValueAsString(cityCountry));
               } catch (JsonProcessingException e) {
                   e.printStackTrace();
               }
           }
       }
    }

    private void testRedisData(List<Integer> ids) {
        try(StatefulRedisConnection<String,String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for(Integer id : ids) {
                    String value = sync.get(String.valueOf(id));
                try {
                    mapper.readValue(value, CityCountry.class);
                } catch(JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void testMysqlData(List<Integer> ids) {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            for (Integer id : ids) {
                City city = cityDAO.getById(id);
                Set<CountryLanguage> languages = city.getCountry().getLanguages();
            }
            session.getTransaction().commit();
        }
    }

    public static void main(String[] args) {
        Main run = new Main();
        List<City> allCities = run.fetchData(run);
        List<CityCountry> preparedData = run.transformData(allCities);
        run.pushToRedis(preparedData);

         //закроем текущую сессию, чтоб точно делать запрос к БД, а не вытянуть данные из кэша
        run.sessionFactory.getCurrentSession().close();

        //выбираем случайных 10 id городов
        //так как мы не делали обработку невалидных ситуаций, использовать существующие в БД id
        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        run.testRedisData(ids);

        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        run.testMysqlData(ids);

        long stopMysql = System.currentTimeMillis();

        System.out.printf("%s:\t%d ms\n", "Redis", (stopRedis - startRedis));
        System.out.printf("%s:\t%d ms\n", "MySQL", (stopMysql - startMysql));
        run.shutDown();
    }
}