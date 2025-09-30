--
-- PostgreSQL database dump
--

\restrict LJ3yywIa8qzr28l3DbleyTnnTsupwzOxbwb285ZoIa9KROW10VVqOAdPiRvSsrE

-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.6

-- Started on 2025-09-22 15:10:25

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 218 (class 1259 OID 16617)
-- Name: enrollments; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.enrollments (
    enroll_id integer NOT NULL,
    student_id integer,
    course text NOT NULL
);


ALTER TABLE public.enrollments OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 16616)
-- Name: enrollments_enroll_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.enrollments_enroll_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.enrollments_enroll_id_seq OWNER TO postgres;

--
-- TOC entry 4907 (class 0 OID 0)
-- Dependencies: 217
-- Name: enrollments_enroll_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.enrollments_enroll_id_seq OWNED BY public.enrollments.enroll_id;


--
-- TOC entry 220 (class 1259 OID 16651)
-- Name: students; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.students (
    id integer NOT NULL,
    name text NOT NULL,
    age integer NOT NULL,
    major text NOT NULL,
    email character varying(100)
);


ALTER TABLE public.students OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 16650)
-- Name: students_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.students_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.students_id_seq OWNER TO postgres;

--
-- TOC entry 4908 (class 0 OID 0)
-- Dependencies: 219
-- Name: students_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.students_id_seq OWNED BY public.students.id;


--
-- TOC entry 4747 (class 2604 OID 16620)
-- Name: enrollments enroll_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.enrollments ALTER COLUMN enroll_id SET DEFAULT nextval('public.enrollments_enroll_id_seq'::regclass);


--
-- TOC entry 4748 (class 2604 OID 16654)
-- Name: students id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.students ALTER COLUMN id SET DEFAULT nextval('public.students_id_seq'::regclass);


--
-- TOC entry 4899 (class 0 OID 16617)
-- Dependencies: 218
-- Data for Name: enrollments; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.enrollments (enroll_id, student_id, course) FROM stdin;
1	2	CPSC 453
2	3	SENG 550
3	4	SENG 550
4	4	ENSF 460
\.


--
-- TOC entry 4901 (class 0 OID 16651)
-- Dependencies: 220
-- Data for Name: students; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.students (id, name, age, major, email) FROM stdin;
3	Luke	21	Engineering	\N
4	Matthew	18	Engineering	\N
1	John	19	Arts	john@icloud.com
2	Mark	27	Computer Science	mark@icloud.com
\.


--
-- TOC entry 4909 (class 0 OID 0)
-- Dependencies: 217
-- Name: enrollments_enroll_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.enrollments_enroll_id_seq', 4, true);


--
-- TOC entry 4910 (class 0 OID 0)
-- Dependencies: 219
-- Name: students_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.students_id_seq', 4, true);


--
-- TOC entry 4750 (class 2606 OID 16624)
-- Name: enrollments enrollments_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.enrollments
    ADD CONSTRAINT enrollments_pkey PRIMARY KEY (enroll_id);


--
-- TOC entry 4752 (class 2606 OID 16658)
-- Name: students students_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.students
    ADD CONSTRAINT students_pkey PRIMARY KEY (id);


-- Completed on 2025-09-22 15:10:26

--
-- PostgreSQL database dump complete
--

\unrestrict LJ3yywIa8qzr28l3DbleyTnnTsupwzOxbwb285ZoIa9KROW10VVqOAdPiRvSsrE

