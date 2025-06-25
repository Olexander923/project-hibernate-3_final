package com.javarush.domain;

public enum Continent {
    ASIA("0-ASIA"),
    EUROPE("1E-EUROPE"),
    NORTH_AMERICA("2-NORTH_AMERICA"),
    AFRICA("3-AFRICA"),
    OCEANIA("4-OCEANIA"),
    ANTARCTICA("5-ANTARCTICA"),
    SOUTH_AMERICA("6-SOUTH_AMERICA");

        private final String value;

        Continent(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }


