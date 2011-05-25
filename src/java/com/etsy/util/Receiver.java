package com.etsy.util;

public interface Receiver<T> {
  void receive(T obj);
}
