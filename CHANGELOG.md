# Changelog

## [0.5.1](https://github.com/Tsanton/pyflake-client/compare/v0.5.0...v0.5.1) (2023-04-13)


### Bug Fixes

* broken import in client ([1886cc5](https://github.com/Tsanton/pyflake-client/commit/1886cc56af1b788235030ea30cd716a73dd6a3b2))

## [0.5.0](https://github.com/Tsanton/pyflake-client/compare/v0.4.0...v0.5.0) (2023-04-13)


### ⚠ BREAKING CHANGES

* alter order of table, role, database role and tag ownership
* warehouse grant and role grant testing
* future grants
* principal descendants refactored
* principal ascendants
* role inheritance refactored
* removing unused grants
* refactor grants a bit
* add grant and future_grant
* add GrantAction and AccountGrant
* use entity and asset interfaces for default load_from_sf implementations
* refactor entities
* refactor entity loading
* change not_null -> nullable, adjust tests
* refactor table and columns
* extend databaserole with principle interface
* extend databaserole with principle interface

### Features

* add grant and future_grant ([881caa9](https://github.com/Tsanton/pyflake-client/commit/881caa9d14504ec69b73b86e41d10afc98840ab7))
* add GrantAction and AccountGrant ([1bdac50](https://github.com/Tsanton/pyflake-client/commit/1bdac50abb525f3557cea477171e57a7621502c2))
* add principle interface to schema ([d1a1cad](https://github.com/Tsanton/pyflake-client/commit/d1a1cad07f030be43a77a62939163a9000d2727b))
* add rest of the column types ([65a18e1](https://github.com/Tsanton/pyflake-client/commit/65a18e1f1167ee19ba5a38250c3f56784f73a107))
* add tag classes ([db94833](https://github.com/Tsanton/pyflake-client/commit/db9483319a25e6d0d058cbb1242f8bd58afa7f99))
* adjust grants and affected tests ([d8d2db5](https://github.com/Tsanton/pyflake-client/commit/d8d2db57e50a47b994d5a80016c35a8be81f75bc))
* change not_null -&gt; nullable, adjust tests ([48b115d](https://github.com/Tsanton/pyflake-client/commit/48b115d8b4d8148582c01fae53044682bd15e144))
* database grants update ([7e4872a](https://github.com/Tsanton/pyflake-client/commit/7e4872a5e94b3a11993b09037baa371489ee0414))
* extend databaserole with principle interface ([c9acbbe](https://github.com/Tsanton/pyflake-client/commit/c9acbbeac98a09d8921a37027185d7009ad2f3e1))
* extend databaserole with principle interface ([8c540e0](https://github.com/Tsanton/pyflake-client/commit/8c540e0f5d71de570ed37352b5169874bdfeb7ff))
* extend role asset and add database_role ([730f167](https://github.com/Tsanton/pyflake-client/commit/730f167b3f6e9c9bddab5633c5114b4d25d21706))
* extend role asset and add database_role ([f18b42a](https://github.com/Tsanton/pyflake-client/commit/f18b42ad533d2ca7330da9f86159e25c0cbfe004))
* fix tag bugs and add tests ([092c170](https://github.com/Tsanton/pyflake-client/commit/092c1707bec2caee67af0325d8edcfe820fc5ff9))
* refactor entities ([cbd7a3a](https://github.com/Tsanton/pyflake-client/commit/cbd7a3a60508c7dd25e4054167b2e7fc104e8553))
* refactor entity loading ([4c1a574](https://github.com/Tsanton/pyflake-client/commit/4c1a574a0e4f63357bbc53769dd491f3928a7d7e))
* refactor table and columns ([628efc4](https://github.com/Tsanton/pyflake-client/commit/628efc4b10041c6ba4051f3d41c52276d9f1feb7))
* table describe ([7a84271](https://github.com/Tsanton/pyflake-client/commit/7a84271ed0a5c69321265a15b7889f2be99b2115))
* tag support for columns ([f0d491d](https://github.com/Tsanton/pyflake-client/commit/f0d491db44a12d38d271d164ac30a6a73bcd36c1))
* update mergeable entity and test ([b6dbcb0](https://github.com/Tsanton/pyflake-client/commit/b6dbcb0e5353d479c33486b473bd809abcbc0e2c))
* update schema describable and affected tests ([22a322b](https://github.com/Tsanton/pyflake-client/commit/22a322b806ffa35ccfb904404e932c030ea634c1))
* update warehouse and tests ([fc819b4](https://github.com/Tsanton/pyflake-client/commit/fc819b4ee5ab555456762be76f5fb157cf037d42))
* use entity and asset interfaces for default load_from_sf implementations ([5dd682a](https://github.com/Tsanton/pyflake-client/commit/5dd682ab83c4c91887af8f7a482fa07f36d162c4))


### Bug Fixes

* added table tag tests ([212f03e](https://github.com/Tsanton/pyflake-client/commit/212f03e9a3a1f86886fde1b9e5f8aa962bc77ade))
* client decribe_many procedure fix ([bfad3f9](https://github.com/Tsanton/pyflake-client/commit/bfad3f9b598312059be32b2c26f4f58700388ec1))
* refactor grants a bit ([61f29a7](https://github.com/Tsanton/pyflake-client/commit/61f29a760869cc8585485a16b708b6cf0d4fc1fb))
* refactoring class renaming error ([00ae284](https://github.com/Tsanton/pyflake-client/commit/00ae2842ebcc07520fe9e7866501c170d4b5a8c9))
* remove buggy fixtures ([2a0fb38](https://github.com/Tsanton/pyflake-client/commit/2a0fb3835ed6a539451b6dc5ec636262a6f5532a))
* remove devcontainer from gitignore ([afb3da1](https://github.com/Tsanton/pyflake-client/commit/afb3da1b3a36bc3a79ba17308c4af7ffb8916dce))
* removing unused grants ([810dcff](https://github.com/Tsanton/pyflake-client/commit/810dcffe0f44841beb279de8426970cc8750fff5))
* sp error, fix schema tests ([8191e04](https://github.com/Tsanton/pyflake-client/commit/8191e04e1603c11ca7da314975efc245608b581a))
* table, wh, db should not be principals ([154dd71](https://github.com/Tsanton/pyflake-client/commit/154dd7197448cb7ef8c9e45f370af1592978be98))
* update role_account_grant tests ([64a6217](https://github.com/Tsanton/pyflake-client/commit/64a621714b2357ef90434c08f6937edd744f812c))
* update schema tests ([4c777f4](https://github.com/Tsanton/pyflake-client/commit/4c777f4d54c7a872bfff8a0b97ddc52497241bf5))
* update tag/classification tag structure ([7aa3c36](https://github.com/Tsanton/pyflake-client/commit/7aa3c36c173ff9be451eb6991e864b467e562ebb))


### Code Refactoring

* alter order of table, role, database role and tag ownership ([1e2b99d](https://github.com/Tsanton/pyflake-client/commit/1e2b99d536c85df70f55d49e7bf2fd5f5df9377e))
* future grants ([83484a7](https://github.com/Tsanton/pyflake-client/commit/83484a78da47bb1a6546da61cd0d0e840a4cef10))
* principal ascendants ([d7499c1](https://github.com/Tsanton/pyflake-client/commit/d7499c1378ada7d8d48fed00aacb539d6f2be923))
* principal descendants refactored ([a26ce09](https://github.com/Tsanton/pyflake-client/commit/a26ce098e397663e224bc295859a11a64f2ce6e6))
* role inheritance refactored ([e3c6449](https://github.com/Tsanton/pyflake-client/commit/e3c644981afcec8114b5127646a22fe8e28ffaca))
* warehouse grant and role grant testing ([eeb4833](https://github.com/Tsanton/pyflake-client/commit/eeb48339a0104a14b4ae76f8ccfc4fa8d4a99ad3))

## [0.4.0](https://github.com/Tsanton/pyflake-client/compare/v0.3.1...v0.4.0) (2023-02-28)


### ⚠ BREAKING CHANGES

* minor bump instead of patch

### Features

* added warehouse and warehouse grant ([1fdd261](https://github.com/Tsanton/pyflake-client/commit/1fdd261518d330662bca093b48667f65ee80879c))
* minor bump instead of patch ([6e4b9a2](https://github.com/Tsanton/pyflake-client/commit/6e4b9a27091c620aca3917f58a548eacaedba424))

## [0.3.1](https://github.com/Tsanton/pyflake-client/compare/v0.3.0...v0.3.1) (2023-02-14)


### Bug Fixes

* remove subtype hinting for 3.8 ([8ab5adc](https://github.com/Tsanton/pyflake-client/commit/8ab5adc19026f282082b6c00ae6d8ef4f5cb8e8e))

## [0.3.0](https://github.com/Tsanton/pyflake-client/compare/v0.2.0...v0.3.0) (2023-02-14)


### ⚠ BREAKING CHANGES

* pyflake_client moved into client.py

### Features

* added anonymous procedures ([463fc3e](https://github.com/Tsanton/pyflake-client/commit/463fc3eea7e288dc52cb280d9d13129dccfd8ffd))


### Code Refactoring

* pyflake_client moved into client.py ([3bd971b](https://github.com/Tsanton/pyflake-client/commit/3bd971bc51cb95ecea08d9a672a78b2079fdfb3f))

## [0.2.0](https://github.com/Tsanton/pyflake-client/compare/v0.1.1...v0.2.0) (2023-02-12)


### ⚠ BREAKING CHANGES

* Role descendants and ascendants added

### Features

* procedure added ([3eb0b05](https://github.com/Tsanton/pyflake-client/commit/3eb0b052f5fc0af5f064b4c355fcbd352edabb52))
* procedure execution ([2012b28](https://github.com/Tsanton/pyflake-client/commit/2012b281e909e25e96cfb613f7f870ff73fed464))
* Role descendants and ascendants added ([e6547f5](https://github.com/Tsanton/pyflake-client/commit/e6547f5db0043f0cae447199df51a9b21b60336e))

## [0.1.1](https://github.com/Tsanton/pyflake-client/compare/v0.1.0...v0.1.1) (2023-02-10)


### Bug Fixes

* add README for setup.py install ([68365c7](https://github.com/Tsanton/pyflake-client/commit/68365c7785a195cb8a565042e42ef46508c43f41))

## 0.1.0 (2023-02-09)


### Features

* init commit ([300e98f](https://github.com/Tsanton/pyflake-client/commit/300e98f46fe07af3e7a49f4813cd6dafd71a24c9))
