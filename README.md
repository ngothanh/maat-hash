# maat-hash
Rust implementation for consistent hashing.

Named after Ma'at, the ancient Egyptian goddess representing truth, balance, and order. This name reflects the stability and balance inherent in consistent hashing.

Sample code:

````Rust
        //given
        let mut ring = DefaultMaatRing::new(100, 10);
        let server = Server::new(
            String::from("1.10.11.12"),
            61,
            true,
        );
        ring.accept(&server);

        let payload = Payload::new(String::from("test"));
        let request = Request::of(payload);

        //when
        let result = ring.route(&request);

        //then
        assert!(result.is_ok());
        match result {
            Ok(found) => { assert!(found == server) }
            Err(_) => {}
        }
````
